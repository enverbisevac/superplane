package blobs

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/database"
	"github.com/superplanehq/superplane/pkg/models"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
)

// CompleteStoreBlob finalizes a two-phase upload: HEADs the bucket
// object via storage.Get, records the actual size, and flips the DB
// row to ready via models.MarkBlobReady. Idempotent — calling on a
// row that is already ready re-verifies the object and returns OK.
func CompleteStoreBlob(
	ctx context.Context,
	storage blob.Storage,
	orgID string,
	req *pb.CompleteStoreBlobRequest,
) (*pb.CompleteStoreBlobResponse, error) {
	if storage == nil {
		return nil, status.Error(codes.FailedPrecondition, blob.ErrBlobsNotConfigured.Error())
	}
	row, err := loadAuthenticatedBlobByID(ctx, orgID, req)
	if err != nil {
		return nil, err
	}

	// HEAD the bucket object via a Get + immediate close.
	rc, info, err := storage.Get(ctx, row.BlobScope(), row.Path)
	if err != nil {
		if errors.Is(err, blob.ErrBlobNotFound) {
			return nil, status.Error(codes.FailedPrecondition, "bucket object missing — upload was not completed")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to verify bucket object: %v", err))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, 0))
	_ = rc.Close()

	// If the row is already ready, re-verify but do not call MarkBlobReady
	// (which would unnecessarily update timestamps / delete nothing).
	if row.Status == models.BlobStatusReady {
		return &pb.CompleteStoreBlobResponse{Blob: serializeBlob(*row)}, nil
	}

	err = database.Conn().Transaction(func(tx *gorm.DB) error {
		return models.MarkBlobReady(tx, row.ID, info.Size)
	})
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to mark blob ready: %v", err))
	}

	// Reload for the serialized response.
	updated, err := models.FindBlob(row.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to reload blob after mark ready")
	}
	return &pb.CompleteStoreBlobResponse{Blob: serializeBlob(*updated)}, nil
}
