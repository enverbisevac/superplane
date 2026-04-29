package blobs

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/database"
	"github.com/superplanehq/superplane/pkg/models"
)

// newTestStorage returns an in-memory blob.Storage suitable for unit
// tests. The mem:// backend cannot presign, which lets tests exercise
// the streaming-fallback code path; cloud-backed presigning is left
// to integration tests.
func newTestStorage(t *testing.T) blob.Storage {
	t.Helper()
	t.Setenv("BLOB_BUCKET_URL", "") // default mem://
	s, err := blob.NewFromEnv(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// seedPendingBlob inserts a pending blob row at (scope, path) and
// returns the inserted row. Test-only helper that hides the boilerplate
// of computing the object key, opening a transaction, and parsing the
// scope's ID strings into typed pointers.
func seedPendingBlob(t *testing.T, scope blob.Scope, path string) *models.Blob {
	t.Helper()
	objKey, err := blob.ObjectKey(scope, path)
	require.NoError(t, err)

	orgUUID, err := uuid.Parse(scope.OrganizationID)
	require.NoError(t, err)

	input := models.CreatePendingBlobInput{
		OrganizationID: orgUUID,
		ScopeType:      string(scope.Type),
		Path:           path,
		ObjectKey:      objKey,
	}
	if scope.CanvasID != "" {
		cid, err := uuid.Parse(scope.CanvasID)
		require.NoError(t, err)
		input.CanvasID = &cid
	}
	if scope.NodeID != "" {
		n := scope.NodeID
		input.NodeID = &n
	}
	if scope.ExecutionID != "" {
		eid, err := uuid.Parse(scope.ExecutionID)
		require.NoError(t, err)
		input.ExecutionID = &eid
	}

	tx := database.Conn().Begin()
	pending, err := models.CreatePendingBlob(tx, input)
	require.NoError(t, err)
	require.NoError(t, tx.Commit().Error)
	return pending
}

// seedReadyBlob inserts a pending blob and immediately flips it to
// ready with the given size. Returns the reloaded ready row.
func seedReadyBlob(t *testing.T, scope blob.Scope, path string, size int64) *models.Blob {
	t.Helper()
	pending := seedPendingBlob(t, scope, path)

	tx := database.Conn().Begin()
	require.NoError(t, models.MarkBlobReady(tx, pending.ID, size))
	require.NoError(t, tx.Commit().Error)

	reloaded, err := models.FindBlob(pending.ID)
	require.NoError(t, err)
	return reloaded
}
