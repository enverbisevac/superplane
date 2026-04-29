package blobs

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/superplanehq/superplane/pkg/authentication"
	"github.com/superplanehq/superplane/pkg/blob"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
	"github.com/superplanehq/superplane/test/support"
)

func TestStoreBlob_OrganizationScope_StreamFallback(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	req := &pb.StoreBlobRequest{
		Scope: &pb.BlobScope{
			Type:           pb.BlobScopeType_BLOB_SCOPE_ORGANIZATION,
			OrganizationId: r.Organization.ID.String(),
		},
		Path:    "logos/sp.png",
		Options: &pb.PutOptions{ContentType: "image/png"},
	}

	resp, err := StoreBlob(ctx, storage, r.Organization.ID.String(), req)
	require.NoError(t, err)
	require.NotNil(t, resp.Blob)
	require.Equal(t, pb.Blob_STATUS_PENDING, resp.Blob.Status)
	require.Equal(t, "logos/sp.png", resp.Blob.Path)

	require.NotNil(t, resp.Upload)
	// mem:// backend cannot presign — expect stream mode with a
	// relative /api/v1/blobs/{id}:stream URL.
	assert.Equal(t, pb.UploadInstructions_MODE_STREAM, resp.Upload.Mode)
	assert.Equal(t, "PUT", resp.Upload.Method)
	assert.Contains(t, resp.Upload.Url, ":stream")
	assert.Contains(t, resp.Upload.Url, resp.Blob.Id)
}

func TestStoreBlob_RejectsMismatchedOrg(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	req := &pb.StoreBlobRequest{
		Scope: &pb.BlobScope{
			Type:           pb.BlobScopeType_BLOB_SCOPE_ORGANIZATION,
			OrganizationId: uuid.NewString(), // different org
		},
		Path: "foo.txt",
	}

	_, err := StoreBlob(ctx, storage, r.Organization.ID.String(), req)
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, s.Code())
}

func TestStoreBlob_RejectsBadPath(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	req := &pb.StoreBlobRequest{
		Scope: &pb.BlobScope{Type: pb.BlobScopeType_BLOB_SCOPE_ORGANIZATION},
		Path:  "../etc/passwd",
	}

	_, err := StoreBlob(ctx, storage, r.Organization.ID.String(), req)
	s, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, s.Code())
	assert.Contains(t, s.Message(), "invalid blob path")
}

func TestStoreBlob_IfNotExists_RejectsExistingReadyBlob(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	seedReadyBlob(t, scope, "dup.txt", 1)

	req := &pb.StoreBlobRequest{
		Scope: &pb.BlobScope{
			Type:           pb.BlobScopeType_BLOB_SCOPE_ORGANIZATION,
			OrganizationId: r.Organization.ID.String(),
		},
		Path:        "dup.txt",
		IfNotExists: true,
	}
	_, err := StoreBlob(ctx, storage, r.Organization.ID.String(), req)
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.AlreadyExists, s.Code())
}
