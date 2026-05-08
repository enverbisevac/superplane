package public

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/database"
	"github.com/superplanehq/superplane/pkg/models"
	"github.com/superplanehq/superplane/test/support"
)

func newBlobHandlerAndStorage(t *testing.T) (*blobStreamHandler, blob.Storage) {
	t.Helper()
	t.Setenv("BLOB_BUCKET_URL", "")
	s, err := blob.NewFromEnv(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return newBlobStreamHandler(s), s
}

// seedPendingBlob inserts a pending blob row at (scope, path) and
// returns it. Local helper for the stream-handler tests; mirrors the
// equivalent helper in pkg/grpc/actions/blobs but lives here because
// the action-package version is unexported.
func seedPendingBlob(t *testing.T, scope blob.Scope, path string) *models.Blob {
	t.Helper()
	objKey, err := blob.ObjectKey(scope, path)
	require.NoError(t, err)

	orgUUID, err := uuid.Parse(scope.OrganizationID)
	require.NoError(t, err)

	input := models.CreatePendingBlobInput{
		OrganizationID: orgUUID,
		ScopeType:      string(scope.Type),
		Path:           path,
		ObjectKey:      objKey,
	}

	tx := database.Conn().Begin()
	pending, err := models.CreatePendingBlob(tx, input)
	require.NoError(t, err)
	require.NoError(t, tx.Commit().Error)
	return pending
}

// seedReadyBlob inserts a pending row and immediately marks it ready
// with the given size, returning the reloaded row.
func seedReadyBlob(t *testing.T, scope blob.Scope, path string, size int64) *models.Blob {
	t.Helper()
	pending := seedPendingBlob(t, scope, path)

	tx := database.Conn().Begin()
	require.NoError(t, models.MarkBlobReady(tx, pending.ID, size))
	require.NoError(t, tx.Commit().Error)

	reloaded, err := models.FindBlob(pending.ID)
	require.NoError(t, err)
	return reloaded
}

func TestBlobStreamHandler_Upload_FlipsRowToReady(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	h, _ := newBlobHandlerAndStorage(t)

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	pending := seedPendingBlob(t, scope, "stream.txt")

	body := bytes.NewReader([]byte("streamed bytes"))
	req := httptest.NewRequest(http.MethodPut, "/api/v1/blobs/"+pending.ID.String()+":stream", body)
	req.Header.Set("x-organization-id", r.Organization.ID.String())
	req = mux.SetURLVars(req, map[string]string{"id": pending.ID.String()})
	w := httptest.NewRecorder()

	h.Upload(w, req)
	require.Equal(t, http.StatusNoContent, w.Code)

	updated, err := models.FindBlob(pending.ID)
	require.NoError(t, err)
	require.Equal(t, models.BlobStatusReady, updated.Status)
	require.Equal(t, int64(len("streamed bytes")), updated.SizeBytes)
}

func TestBlobStreamHandler_Upload_WrongOrgIsNotFound(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	h, _ := newBlobHandlerAndStorage(t)

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	pending := seedPendingBlob(t, scope, "wrongorg.txt")

	req := httptest.NewRequest(http.MethodPut, "/api/v1/blobs/"+pending.ID.String()+":stream", bytes.NewReader(nil))
	req.Header.Set("x-organization-id", uuid.NewString())
	req = mux.SetURLVars(req, map[string]string{"id": pending.ID.String()})
	w := httptest.NewRecorder()

	h.Upload(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestBlobStreamHandler_Download_ReturnsBytes(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	h, storage := newBlobHandlerAndStorage(t)

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	require.NoError(t, storage.Put(context.Background(), scope, "dl.txt",
		bytes.NewReader([]byte("bytes-out")), blob.PutOptions{ContentType: "text/plain"}))

	ready := seedReadyBlob(t, scope, "dl.txt", 9)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/blobs/"+ready.ID.String()+":download", nil)
	req.Header.Set("x-organization-id", r.Organization.ID.String())
	req = mux.SetURLVars(req, map[string]string{"id": ready.ID.String()})
	w := httptest.NewRecorder()

	h.Download(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	got, err := io.ReadAll(w.Body)
	require.NoError(t, err)
	require.Equal(t, "bytes-out", string(got))
}
