package blobs

import (
	"context"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/models"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
)

// DeleteBlob removes the DB row and best-effort deletes the bucket
// object. A second call with the same ID returns NotFound. Bucket-
// delete failures are logged but do not fail the call — the GC worker
// will clean up orphans on its next sweep.
func DeleteBlob(
	ctx context.Context,
	storage blob.Storage,
	orgID string,
	req *pb.DeleteBlobRequest,
) (*pb.DeleteBlobResponse, error) {
	row, err := loadAuthenticatedBlobByID(ctx, orgID, req)
	if err != nil {
		return nil, err
	}

	if err := models.DeleteBlob(row.ID); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to delete blob row: %v", err))
	}

	if storage != nil {
		if err := storage.Delete(ctx, row.BlobScope(), row.Path); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
			log.Warnf("failed to delete bucket object for blob %s: %v", row.ID, err)
		}
	}
	return &pb.DeleteBlobResponse{}, nil
}
