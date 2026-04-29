package blobs

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/superplanehq/superplane/pkg/authentication"
	"github.com/superplanehq/superplane/pkg/blob"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
	"github.com/superplanehq/superplane/test/support"
)

func TestDownloadBlob_StreamFallbackOnMem(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	require.NoError(t, storage.Put(context.Background(), scope, "dl.txt",
		bytes.NewReader([]byte("content")), blob.PutOptions{}))

	ready := seedReadyBlob(t, scope, "dl.txt", 7)

	resp, err := DownloadBlob(ctx, storage, r.Organization.ID.String(), &pb.DownloadBlobRequest{Id: ready.ID.String()})
	require.NoError(t, err)
	require.NotNil(t, resp.Download)
	assert.Equal(t, pb.UploadInstructions_MODE_STREAM, resp.Download.Mode)
	assert.Equal(t, "GET", resp.Download.Method)
	assert.Contains(t, resp.Download.Url, ready.ID.String())
	assert.Contains(t, resp.Download.Url, ":download")
}

func TestDownloadBlob_PendingIsFailedPrecondition(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	pending := seedPendingBlob(t, scope, "still-pending.txt")

	_, err := DownloadBlob(ctx, storage, r.Organization.ID.String(), &pb.DownloadBlobRequest{Id: pending.ID.String()})
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, s.Code())
}
