package blobs

import (
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

func TestDescribeBlob_ReturnsReadyRow(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	ready := seedReadyBlob(t, scope, "desc.txt", 5)

	resp, err := DescribeBlob(ctx, storage, r.Organization.ID.String(), &pb.DescribeBlobRequest{Id: ready.ID.String()})
	require.NoError(t, err)
	require.Equal(t, ready.ID.String(), resp.Blob.Id)
	require.Equal(t, pb.Blob_STATUS_READY, resp.Blob.Status)
	require.Equal(t, int64(5), resp.Blob.SizeBytes)
}

func TestDescribeBlob_WrongOrgIsNotFound(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	_, err := DescribeBlob(ctx, storage, r.Organization.ID.String(), &pb.DescribeBlobRequest{Id: uuid.NewString()})
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, s.Code())
}

func TestDescribeBlob_BadID(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	_, err := DescribeBlob(ctx, storage, r.Organization.ID.String(), &pb.DescribeBlobRequest{Id: "not-a-uuid"})
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, s.Code())
}
