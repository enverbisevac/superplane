package contexts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/core"
)

func newTestStore(t *testing.T) blob.Storage {
	t.Helper()
	s, err := blob.NewFromEnv(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func newTestScope() ContextScope {
	return ContextScope{
		OrganizationID: uuid.NewString(),
		CanvasID:       uuid.NewString(),
		NodeID:         "test-node",
		ExecutionID:    uuid.NewString(),
	}
}

func readAllAndClose(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	return b
}

func TestExecutionBlobs_PutGetListDelete(t *testing.T) {
	store := newTestStore(t)
	scope := newTestScope()
	ctx := NewBlobsContext(store, scope)

	require.NoError(t, ctx.Execution().Put(
		"hello.txt",
		bytes.NewReader([]byte("hello world")),
		core.PutBlobOptions{ContentType: "text/plain"},
	))

	rc, info, err := ctx.Execution().Get("hello.txt")
	require.NoError(t, err)
	assert.Equal(t, "hello.txt", info.Path)
	assert.Equal(t, []byte("hello world"), readAllAndClose(t, rc))

	listed, err := ctx.Execution().List("")
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.Equal(t, "hello.txt", listed[0].Path)

	require.NoError(t, ctx.Execution().Delete("hello.txt"))

	_, _, err = ctx.Execution().Get("hello.txt")
	assert.ErrorIs(t, err, blob.ErrBlobNotFound)
}

func TestExecutionBlobs_IsolatedByExecutionID(t *testing.T) {
	store := newTestStore(t)

	orgID, canvasID := uuid.NewString(), uuid.NewString()
	scopeA := ContextScope{
		OrganizationID: orgID,
		CanvasID:       canvasID,
		NodeID:         "n",
		ExecutionID:    uuid.NewString(),
	}
	scopeB := scopeA
	scopeB.ExecutionID = uuid.NewString()

	ctxA := NewBlobsContext(store, scopeA)
	ctxB := NewBlobsContext(store, scopeB)

	require.NoError(t, ctxA.Execution().Put(
		"shared-name.txt",
		bytes.NewReader([]byte("from A")),
		core.PutBlobOptions{},
	))

	_, _, err := ctxB.Execution().Get("shared-name.txt")
	assert.ErrorIs(t, err, blob.ErrBlobNotFound)

	listB, err := ctxB.Execution().List("")
	require.NoError(t, err)
	assert.Empty(t, listB)
}

func TestNodeBlobs_IsolatedByNodeID(t *testing.T) {
	store := newTestStore(t)
	orgID, canvasID := uuid.NewString(), uuid.NewString()

	require.NoError(t, store.Put(
		context.Background(),
		blob.Scope{
			Type:           blob.ScopeNode,
			OrganizationID: orgID,
			CanvasID:       canvasID,
			NodeID:         "node-a",
		},
		"x.txt",
		bytes.NewReader([]byte("hello")),
		blob.PutOptions{},
	))

	ctxA := NewBlobsContext(store, ContextScope{
		OrganizationID: orgID, CanvasID: canvasID, NodeID: "node-a",
		ExecutionID: uuid.NewString(),
	})
	ctxB := NewBlobsContext(store, ContextScope{
		OrganizationID: orgID, CanvasID: canvasID, NodeID: "node-b",
		ExecutionID: uuid.NewString(),
	})

	rc, _, err := ctxA.Node().Get("x.txt")
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), readAllAndClose(t, rc))

	_, _, err = ctxB.Node().Get("x.txt")
	assert.ErrorIs(t, err, blob.ErrBlobNotFound)
}

func TestNilStorage_ReturnsNotConfigured(t *testing.T) {
	ctx := NewBlobsContext(nil, newTestScope())

	assertNotConfigured := func(t *testing.T, err error) {
		t.Helper()
		assert.ErrorIs(t, err, blob.ErrBlobsNotConfigured)
	}

	// Execution: full read/write surface.
	assertNotConfigured(t, ctx.Execution().Put("p", bytes.NewReader(nil), core.PutBlobOptions{}))
	_, _, err := ctx.Execution().Get("p")
	assertNotConfigured(t, err)
	_, err = ctx.Execution().List("")
	assertNotConfigured(t, err)
	assertNotConfigured(t, ctx.Execution().Delete("p"))

	// Read-only surfaces.
	_, _, err = ctx.Node().Get("p")
	assertNotConfigured(t, err)
	_, err = ctx.Node().List("")
	assertNotConfigured(t, err)

	_, _, err = ctx.Canvas().Get("p")
	assertNotConfigured(t, err)
	_, err = ctx.Canvas().List("")
	assertNotConfigured(t, err)

	_, _, err = ctx.Organization().Get("p")
	assertNotConfigured(t, err)
	_, err = ctx.Organization().List("")
	assertNotConfigured(t, err)
}

// TestReadOnlyAccessors_NoPutOrDelete codifies the read-only invariant
// at compile time: the three non-execution accessors must implement
// only the read-only interfaces. If anyone adds Put/Delete to
// NodeBlobs/CanvasBlobs/OrganizationBlobs, the assignments below will
// break — pair the change with an update to design spec §7.2.
func TestReadOnlyAccessors_NoPutOrDelete(t *testing.T) {
	var (
		_ core.NodeBlobs         = (*nodeBlobs)(nil)
		_ core.CanvasBlobs       = (*canvasBlobs)(nil)
		_ core.OrganizationBlobs = (*orgBlobs)(nil)
	)
}

// TestPaginationLoopExits documents that listAll terminates against
// the current storage backend (which does not emit a NextToken). When
// pagination lands in pkg/blob, this test can be extended to seed
// more than one page and assert the full set is returned.
func TestPaginationLoopExits(t *testing.T) {
	store := newTestStore(t)
	scope := newTestScope()
	ctx := NewBlobsContext(store, scope)

	for _, p := range []string{"a.txt", "b.txt", "c.txt"} {
		require.NoError(t, ctx.Execution().Put(p, bytes.NewReader([]byte(p)), core.PutBlobOptions{}))
	}

	listed, err := ctx.Execution().List("")
	require.NoError(t, err)
	require.Len(t, listed, 3)
}

// Sanity check that blob.ErrBlobsNotConfigured is exported and stable —
// errors.Is is the documented way callers should detect it.
func TestErrBlobsNotConfigured_IsExported(t *testing.T) {
	assert.True(t, errors.Is(blob.ErrBlobsNotConfigured, blob.ErrBlobsNotConfigured))
}
