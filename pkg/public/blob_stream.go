package public

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/database"
	"github.com/superplanehq/superplane/pkg/models"
)

// blobStreamHandler serves the fallback upload/download endpoints for
// backends that cannot issue presigned URLs (mem://, file://).
//
//	PUT  /api/v1/blobs/{id}:stream    — upload bytes into a pending blob
//	GET  /api/v1/blobs/{id}:download  — stream bytes out of a ready blob
//
// The handler relies on the organization-auth middleware (the same
// protectedGRPCHandler wrapper that guards /api/v1/blobs) running
// *before* it — it reads x-user-id and x-organization-id from the
// request headers, verifies the blob belongs to the authenticated
// organization, and only then touches storage.
type blobStreamHandler struct {
	storage blob.Storage
}

func newBlobStreamHandler(storage blob.Storage) *blobStreamHandler {
	return &blobStreamHandler{
		storage: storage,
	}
}

// Upload handles PUT /api/v1/blobs/{id}:stream. The body is streamed
// into blob.Storage.Put; on success, the pending row is flipped to
// ready via models.MarkBlobReady. The total number of bytes written
// is recorded as size_bytes.
func (h *blobStreamHandler) Upload(w http.ResponseWriter, r *http.Request) {
	if h.storage == nil {
		http.Error(w, blob.ErrBlobsNotConfigured.Error(), http.StatusServiceUnavailable)
		return
	}

	row, ok := h.loadBlob(w, r)
	if !ok {
		return
	}
	if row.Status == models.BlobStatusReady {
		http.Error(w, "blob is already finalized", http.StatusConflict)
		return
	}

	counter := &countingReader{r: r.Body}
	putOpts := blob.PutOptions{}
	if row.ContentType != nil {
		putOpts.ContentType = *row.ContentType
	}
	if err := h.storage.Put(r.Context(), row.BlobScope(), row.Path, counter, putOpts); err != nil {
		log.Warnf("stream upload failed for blob %s: %v", row.ID, err)
		http.Error(w, "upload failed", http.StatusInternalServerError)
		return
	}

	err := database.Conn().Transaction(func(tx *gorm.DB) error {
		return models.MarkBlobReady(tx, row.ID, counter.n)
	})
	if err != nil {
		log.Warnf("mark-ready failed for blob %s (bytes already in bucket): %v", row.ID, err)
		http.Error(w, "finalize failed", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Download handles GET /api/v1/blobs/{id}:download. Streams the
// bucket object into the HTTP response.
func (h *blobStreamHandler) Download(w http.ResponseWriter, r *http.Request) {
	if h.storage == nil {
		http.Error(w, blob.ErrBlobsNotConfigured.Error(), http.StatusServiceUnavailable)
		return
	}
	row, ok := h.loadBlob(w, r)
	if !ok {
		return
	}
	if row.Status != models.BlobStatusReady {
		http.Error(w, "blob is not ready", http.StatusPreconditionFailed)
		return
	}

	rc, info, err := h.storage.Get(r.Context(), row.BlobScope(), row.Path)
	if err != nil {
		if errors.Is(err, blob.ErrBlobNotFound) {
			http.Error(w, "blob object missing", http.StatusNotFound)
			return
		}
		http.Error(w, "read failed", http.StatusInternalServerError)
		return
	}
	defer rc.Close()

	if info.ContentType != "" {
		w.Header().Set("Content-Type", info.ContentType)
	} else if row.ContentType != nil {
		w.Header().Set("Content-Type", *row.ContentType)
	}
	if info.Size > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

// loadBlob parses {id}, verifies the caller's org owns it, and
// returns the row. Writes an appropriate HTTP error and returns
// ok=false on any miss.
func (h *blobStreamHandler) loadBlob(w http.ResponseWriter, r *http.Request) (*models.Blob, bool) {
	vars := mux.Vars(r)
	idStr := vars["id"]
	if idStr == "" {
		http.Error(w, "missing blob id", http.StatusBadRequest)
		return nil, false
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid blob id", http.StatusBadRequest)
		return nil, false
	}
	orgHeader := r.Header.Get("x-organization-id")
	if orgHeader == "" {
		// The org-auth middleware should have rejected this before
		// we got here; treat the missing header as not-found to avoid
		// leaking whether the blob exists.
		http.Error(w, "not found", http.StatusNotFound)
		return nil, false
	}
	orgUUID, err := uuid.Parse(orgHeader)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return nil, false
	}

	row, err := models.FindBlob(id)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return nil, false
	}
	if row.OrganizationID != orgUUID {
		http.Error(w, "not found", http.StatusNotFound)
		return nil, false
	}
	return row, true
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
