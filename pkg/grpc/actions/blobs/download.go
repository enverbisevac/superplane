package blobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/models"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
)

// DownloadBlob returns download instructions for a READY blob —
// either a presigned GET URL (cloud backends) or a stream URL that
// the client fetches from the API.
func DownloadBlob(
	ctx context.Context,
	storage blob.Storage,
	orgID string,
	req *pb.DownloadBlobRequest,
) (*pb.DownloadBlobResponse, error) {
	if storage == nil {
		return nil, status.Error(codes.FailedPrecondition, blob.ErrBlobsNotConfigured.Error())
	}
	row, err := loadAuthenticatedBlobByID(ctx, orgID, req)
	if err != nil {
		return nil, err
	}
	if row.Status != models.BlobStatusReady {
		return nil, status.Error(codes.FailedPrecondition, "blob is not ready")
	}

	ttl := time.Duration(clampTTL(req.UrlTtlSeconds)) * time.Second
	instructions, err := buildDownloadInstructions(ctx, storage, row.BlobScope(), row.Path, ttl, row.ID)
	if err != nil {
		return nil, err
	}
	return &pb.DownloadBlobResponse{
		Blob:     serializeBlob(*row),
		Download: instructions,
	}, nil
}

// buildDownloadInstructions asks the storage backend for a presigned
// GET URL. Returns stream-mode instructions if the backend cannot
// presign. Mirrors buildUploadInstructions in store.go.
func buildDownloadInstructions(
	ctx context.Context,
	storage blob.Storage,
	scope blob.Scope,
	path string,
	ttl time.Duration,
	blobID uuid.UUID,
) (*pb.DownloadInstructions, error) {
	signed, err := storage.PresignGet(ctx, scope, path, ttl)
	if err == nil {
		return &pb.DownloadInstructions{
			Url:       signed.URL,
			Method:    signed.Method,
			ExpiresAt: timestamppb.New(signed.ExpiresAt),
			Mode:      pb.UploadInstructions_MODE_PRESIGNED,
		}, nil
	}
	if !errors.Is(err, blob.ErrPresignNotSupported) {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to presign download: %v", err))
	}
	return &pb.DownloadInstructions{
		Url:       buildStreamPath(blobID, "download"),
		Method:    "GET",
		ExpiresAt: timestamppb.New(time.Now().Add(ttl)),
		Mode:      pb.UploadInstructions_MODE_STREAM,
	}, nil
}
