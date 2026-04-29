package blobs

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/superplanehq/superplane/pkg/authentication"
	"github.com/superplanehq/superplane/pkg/blob"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
	"github.com/superplanehq/superplane/test/support"
)

func TestCompleteStoreBlob_FlipsReady(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	// Create pending row + actually write bytes into the mem bucket
	// so the HEAD check in CompleteStoreBlob can find them.
	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	pending := seedPendingBlob(t, scope, "greetings/en.txt")

	require.NoError(t, storage.Put(context.Background(), scope, "greetings/en.txt",
		bytes.NewReader([]byte("hello world")), blob.PutOptions{ContentType: "text/plain"}))

	resp, err := CompleteStoreBlob(ctx, storage, r.Organization.ID.String(), &pb.CompleteStoreBlobRequest{Id: pending.ID.String()})
	require.NoError(t, err)
	require.Equal(t, pb.Blob_STATUS_READY, resp.Blob.Status)
	require.Equal(t, int64(len("hello world")), resp.Blob.SizeBytes)
}

func TestCompleteStoreBlob_ObjectMissingFails(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	pending := seedPendingBlob(t, scope, "missing.txt")

	_, err := CompleteStoreBlob(ctx, storage, r.Organization.ID.String(), &pb.CompleteStoreBlobRequest{Id: pending.ID.String()})
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, s.Code())
}

func TestCompleteStoreBlob_IsIdempotentOnReady(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	ready := seedReadyBlob(t, scope, "twice.txt", 3)

	require.NoError(t, storage.Put(context.Background(), scope, "twice.txt",
		bytes.NewReader([]byte("xyz")), blob.PutOptions{}))

	resp, err := CompleteStoreBlob(ctx, storage, r.Organization.ID.String(), &pb.CompleteStoreBlobRequest{Id: ready.ID.String()})
	require.NoError(t, err)
	require.Equal(t, pb.Blob_STATUS_READY, resp.Blob.Status)
	require.Equal(t, int64(3), resp.Blob.SizeBytes)
}

func TestCompleteStoreBlob_WrongOrgReturnsNotFound(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	_, err := CompleteStoreBlob(ctx, storage, r.Organization.ID.String(), &pb.CompleteStoreBlobRequest{Id: uuid.NewString()})
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, s.Code())
}
