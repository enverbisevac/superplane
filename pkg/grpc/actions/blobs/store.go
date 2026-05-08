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
	"gorm.io/gorm"

	"github.com/superplanehq/superplane/pkg/authentication"
	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/database"
	"github.com/superplanehq/superplane/pkg/models"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
)

// StoreBlob begins a two-phase blob upload. It:
//  1. resolves and validates the scope against the authenticated org;
//  2. (if IfNotExists) rejects if a READY blob already exists at path;
//  3. computes the canonical object_key via blob.ObjectKey;
//  4. inserts a pending DB row via models.CreatePendingBlob;
//  5. asks the storage backend to presign a PUT URL. If the backend
//     returns ErrPresignNotSupported, falls back to a stream URL
//     relative to the API host (/api/v1/blobs/{id}:stream).
func StoreBlob(
	ctx context.Context,
	storage blob.Storage,
	orgID string,
	req *pb.StoreBlobRequest,
) (*pb.StoreBlobResponse, error) {
	if storage == nil {
		return nil, status.Error(codes.FailedPrecondition, blob.ErrBlobsNotConfigured.Error())
	}
	userID, userIsSet := authentication.GetUserIdFromMetadata(ctx)
	if !userIsSet {
		return nil, status.Error(codes.Unauthenticated, "user not authenticated")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}

	resolved, err := resolveAndValidateScope(ctx, orgID, req.Scope)
	if err != nil {
		return nil, err
	}

	objKey, err := blob.ObjectKey(resolved.BlobScope, req.Path)
	if err != nil {
		if errors.Is(err, blob.ErrInvalidPath) || errors.Is(err, blob.ErrInvalidScope) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	if req.IfNotExists {
		existing, err := models.FindBlobByPath(
			mustParseUUID(orgID),
			resolved.ModelType,
			resolved.CanvasID,
			resolved.NodeID,
			resolved.ExecutionID,
			req.Path,
		)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.Internal, "failed to check existing blob")
		}
		if existing != nil {
			return nil, status.Error(codes.AlreadyExists, "a blob already exists at this path")
		}
	}

	var contentType *string
	if req.Options != nil && req.Options.ContentType != "" {
		ct := req.Options.ContentType
		contentType = &ct
	}

	userUUID, uerr := uuid.Parse(userID)
	var createdBy *uuid.UUID
	if uerr == nil {
		createdBy = &userUUID
	}

	var pending *models.Blob
	err = database.Conn().Transaction(func(tx *gorm.DB) error {
		b, createErr := models.CreatePendingBlob(tx, models.CreatePendingBlobInput{
			OrganizationID: mustParseUUID(orgID),
			ScopeType:      resolved.ModelType,
			CanvasID:       resolved.CanvasID,
			NodeID:         resolved.NodeID,
			ExecutionID:    resolved.ExecutionID,
			Path:           req.Path,
			ObjectKey:      objKey,
			ContentType:    contentType,
			CreatedBy:      createdBy,
		})
		if createErr != nil {
			return createErr
		}
		pending = b
		return nil
	})
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create pending blob: %v", err))
	}

	ttl := time.Duration(clampTTL(req.UrlTtlSeconds)) * time.Second

	instructions, err := buildUploadInstructions(ctx, storage, resolved.BlobScope, req.Path, req.Options, ttl, pending.ID)
	if err != nil {
		// Best-effort cleanup: delete the pending row so the caller
		// can retry without colliding on object_key uniqueness.
		_ = models.DeleteBlob(pending.ID)
		return nil, err
	}

	return &pb.StoreBlobResponse{
		Blob:   serializeBlob(*pending),
		Upload: instructions,
	}, nil
}

// buildUploadInstructions asks the storage backend for a presigned
// PUT URL. Returns stream-mode instructions if the backend cannot
// presign.
func buildUploadInstructions(
	ctx context.Context,
	storage blob.Storage,
	scope blob.Scope,
	path string,
	opts *pb.PutOptions,
	ttl time.Duration,
	blobID uuid.UUID,
) (*pb.UploadInstructions, error) {
	putOpts := blob.PutOptions{}
	if opts != nil {
		putOpts.ContentType = opts.ContentType
	}

	signed, err := storage.PresignPut(ctx, scope, path, putOpts, ttl)
	if err == nil {
		return &pb.UploadInstructions{
			Url:       signed.URL,
			Method:    signed.Method,
			Headers:   signed.Headers,
			ExpiresAt: timestamppb.New(signed.ExpiresAt),
			Mode:      pb.UploadInstructions_MODE_PRESIGNED,
		}, nil
	}
	if !errors.Is(err, blob.ErrPresignNotSupported) {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to presign upload: %v", err))
	}

	return &pb.UploadInstructions{
		Url:       buildStreamPath(blobID, "stream"),
		Method:    "PUT",
		ExpiresAt: timestamppb.New(time.Now().Add(ttl)),
		Mode:      pb.UploadInstructions_MODE_STREAM,
	}, nil
}

func mustParseUUID(s string) uuid.UUID {
	// Only used for values we've already validated as UUIDs (org from
	// the auth context). Should never panic in production.
	return uuid.MustParse(s)
}
