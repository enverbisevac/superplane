package blobs

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/models"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
)

func TestScopeTypeToModel(t *testing.T) {
	cases := map[pb.BlobScopeType]string{
		pb.BlobScopeType_BLOB_SCOPE_ORGANIZATION: models.BlobScopeOrganization,
		pb.BlobScopeType_BLOB_SCOPE_CANVAS:       models.BlobScopeCanvas,
		pb.BlobScopeType_BLOB_SCOPE_NODE:         models.BlobScopeNode,
		pb.BlobScopeType_BLOB_SCOPE_EXECUTION:    models.BlobScopeExecution,
	}
	for in, want := range cases {
		got, err := scopeTypeToModel(in)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	_, err := scopeTypeToModel(pb.BlobScopeType_BLOB_SCOPE_UNSPECIFIED)
	require.Error(t, err)
}

func TestScopeTypeToBlobPackage(t *testing.T) {
	got, err := scopeTypeToBlobPackage(pb.BlobScopeType_BLOB_SCOPE_CANVAS)
	require.NoError(t, err)
	require.Equal(t, blob.ScopeCanvas, got)
}

func TestScopeTypeFromModel(t *testing.T) {
	require.Equal(t, pb.BlobScopeType_BLOB_SCOPE_CANVAS, scopeTypeFromModel(models.BlobScopeCanvas))
	require.Equal(t, pb.BlobScopeType_BLOB_SCOPE_UNSPECIFIED, scopeTypeFromModel("garbage"))
}

func TestSerializeBlob_ReadyRow(t *testing.T) {
	canvasID := uuid.New()
	ct := "text/plain"
	row := models.Blob{
		ID:             uuid.New(),
		OrganizationID: uuid.New(),
		ScopeType:      models.BlobScopeCanvas,
		CanvasID:       &canvasID,
		Path:           "configs/prod.yaml",
		ObjectKey:      "blobs/xyz/canvas/abc/configs/prod.yaml",
		SizeBytes:      42,
		ContentType:    &ct,
		Status:         models.BlobStatusReady,
	}
	got := serializeBlob(row)
	require.Equal(t, row.ID.String(), got.Id)
	require.Equal(t, pb.Blob_STATUS_READY, got.Status)
	require.Equal(t, pb.BlobScopeType_BLOB_SCOPE_CANVAS, got.Scope.Type)
	require.Equal(t, canvasID.String(), got.Scope.CanvasId)
	require.Equal(t, int64(42), got.SizeBytes)
	require.Equal(t, "text/plain", got.ContentType)
}
