package blobs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/superplanehq/superplane/pkg/authentication"
	"github.com/superplanehq/superplane/pkg/blob"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
	"github.com/superplanehq/superplane/test/support"
)

func TestListBlobs_OrganizationScope_NewestFirst(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	seedReadyBlob(t, scope, "first.txt", 1)
	seedReadyBlob(t, scope, "second.txt", 2)

	req := &pb.ListBlobsRequest{
		Scope: &pb.BlobScope{
			Type:           pb.BlobScopeType_BLOB_SCOPE_ORGANIZATION,
			OrganizationId: r.Organization.ID.String(),
		},
	}
	resp, err := ListBlobs(ctx, storage, r.Organization.ID.String(), req)
	require.NoError(t, err)
	require.Len(t, resp.Blobs, 2)
	require.Equal(t, "second.txt", resp.Blobs[0].Path)
	require.Equal(t, "first.txt", resp.Blobs[1].Path)
}

func TestListBlobs_HidesPendingRows(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	seedPendingBlob(t, scope, "pending-only.txt")

	req := &pb.ListBlobsRequest{
		Scope: &pb.BlobScope{
			Type:           pb.BlobScopeType_BLOB_SCOPE_ORGANIZATION,
			OrganizationId: r.Organization.ID.String(),
		},
	}
	resp, err := ListBlobs(ctx, storage, r.Organization.ID.String(), req)
	require.NoError(t, err)
	require.Empty(t, resp.Blobs)
}
