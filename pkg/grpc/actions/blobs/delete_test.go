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
	"github.com/superplanehq/superplane/pkg/models"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
	"github.com/superplanehq/superplane/test/support"
)

func TestDeleteBlob_RemovesRowAndObject(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	scope := blob.Scope{Type: blob.ScopeOrganization, OrganizationID: r.Organization.ID.String()}
	ready := seedReadyBlob(t, scope, "doomed.txt", 1)

	_, err := DeleteBlob(ctx, storage, r.Organization.ID.String(), &pb.DeleteBlobRequest{Id: ready.ID.String()})
	require.NoError(t, err)

	_, err = models.FindBlob(ready.ID)
	require.Error(t, err) // row is gone
}

func TestDeleteBlob_SecondCallReturnsNotFound(t *testing.T) {
	r := support.SetupWithOptions(t, support.SetupOptions{})
	storage := newTestStorage(t)
	ctx := authentication.SetUserIdInMetadata(context.Background(), r.User.String())

	_, err := DeleteBlob(ctx, storage, r.Organization.ID.String(), &pb.DeleteBlobRequest{Id: uuid.NewString()})
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, s.Code())
}
