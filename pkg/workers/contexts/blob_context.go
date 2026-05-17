package contexts

import (
	"context"
	"io"

	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/core"
)

// ContextScope captures the four scope IDs for a single execution.
// It is built in NodeExecutor from the execution's own DB records so
// components cannot inject or forge any of these values.
type ContextScope struct {
	OrganizationID string
	CanvasID       string
	NodeID         string
	ExecutionID    string
}

// blobsContext is the root of the BlobsContext tree. The four scope
// accessors each return a small struct that closes over the storage
// handle plus the relevant subset of scope IDs.
type blobsContext struct {
	store blob.Storage
	scope ContextScope
}

// NewBlobsContext returns a BlobsContext bound to the given scope. A nil
// store is supported — every method on the returned accessors will
// return blob.ErrBlobsNotConfigured, so operators can run without
// configuring blob storage.
func NewBlobsContext(store blob.Storage, scope ContextScope) core.BlobsContext {
	return &blobsContext{store: store, scope: scope}
}

func (c *blobsContext) Execution() core.ExecutionBlobs {
	return &executionBlobs{store: c.store, scope: c.scope}
}

func (c *blobsContext) Node() core.NodeBlobs {
	return &nodeBlobs{store: c.store, scope: c.scope}
}

func (c *blobsContext) Canvas() core.CanvasBlobs {
	return &canvasBlobs{store: c.store, scope: c.scope}
}

func (c *blobsContext) Organization() core.OrganizationBlobs {
	return &orgBlobs{store: c.store, scope: c.scope}
}

type executionBlobs struct {
	store blob.Storage
	scope ContextScope
}

func (e *executionBlobs) blobScope() blob.Scope {
	return blob.Scope{
		Type:           blob.ScopeExecution,
		OrganizationID: e.scope.OrganizationID,
		ExecutionID:    e.scope.ExecutionID,
	}
}

func (e *executionBlobs) Put(path string, body io.Reader, opts core.PutBlobOptions) error {
	if e.store == nil {
		return blob.ErrBlobsNotConfigured
	}
	return e.store.Put(context.Background(), e.blobScope(), path, body, opts)
}

func (e *executionBlobs) Get(path string) (io.ReadCloser, core.BlobInfo, error) {
	if e.store == nil {
		return nil, core.BlobInfo{}, blob.ErrBlobsNotConfigured
	}
	return e.store.Get(context.Background(), e.blobScope(), path)
}

func (e *executionBlobs) List(prefix string) ([]core.BlobInfo, error) {
	if e.store == nil {
		return nil, blob.ErrBlobsNotConfigured
	}
	return listAll(context.Background(), e.store, e.blobScope(), prefix)
}

func (e *executionBlobs) Delete(path string) error {
	if e.store == nil {
		return blob.ErrBlobsNotConfigured
	}
	return e.store.Delete(context.Background(), e.blobScope(), path)
}

type nodeBlobs struct {
	store blob.Storage
	scope ContextScope
}

func (n *nodeBlobs) blobScope() blob.Scope {
	return blob.Scope{
		Type:           blob.ScopeNode,
		OrganizationID: n.scope.OrganizationID,
		CanvasID:       n.scope.CanvasID,
		NodeID:         n.scope.NodeID,
	}
}

func (n *nodeBlobs) Get(path string) (io.ReadCloser, core.BlobInfo, error) {
	if n.store == nil {
		return nil, core.BlobInfo{}, blob.ErrBlobsNotConfigured
	}
	return n.store.Get(context.Background(), n.blobScope(), path)
}

func (n *nodeBlobs) List(prefix string) ([]core.BlobInfo, error) {
	if n.store == nil {
		return nil, blob.ErrBlobsNotConfigured
	}
	return listAll(context.Background(), n.store, n.blobScope(), prefix)
}

type canvasBlobs struct {
	store blob.Storage
	scope ContextScope
}

func (c *canvasBlobs) blobScope() blob.Scope {
	return blob.Scope{
		Type:           blob.ScopeCanvas,
		OrganizationID: c.scope.OrganizationID,
		CanvasID:       c.scope.CanvasID,
	}
}

func (c *canvasBlobs) Get(path string) (io.ReadCloser, core.BlobInfo, error) {
	if c.store == nil {
		return nil, core.BlobInfo{}, blob.ErrBlobsNotConfigured
	}
	return c.store.Get(context.Background(), c.blobScope(), path)
}

func (c *canvasBlobs) List(prefix string) ([]core.BlobInfo, error) {
	if c.store == nil {
		return nil, blob.ErrBlobsNotConfigured
	}
	return listAll(context.Background(), c.store, c.blobScope(), prefix)
}

type orgBlobs struct {
	store blob.Storage
	scope ContextScope
}

func (o *orgBlobs) blobScope() blob.Scope {
	return blob.Scope{
		Type:           blob.ScopeOrganization,
		OrganizationID: o.scope.OrganizationID,
	}
}

func (o *orgBlobs) Get(path string) (io.ReadCloser, core.BlobInfo, error) {
	if o.store == nil {
		return nil, core.BlobInfo{}, blob.ErrBlobsNotConfigured
	}
	return o.store.Get(context.Background(), o.blobScope(), path)
}

func (o *orgBlobs) List(prefix string) ([]core.BlobInfo, error) {
	if o.store == nil {
		return nil, blob.ErrBlobsNotConfigured
	}
	return listAll(context.Background(), o.store, o.blobScope(), prefix)
}

// listAll paginates through all blobs under the given scope+prefix. The
// component-runtime API hides pagination because components want the
// full list; the paginated form is for the gRPC layer that has a UI to
// render. MaxResults is left at zero so the storage layer picks its
// own default page size.
func listAll(ctx context.Context, store blob.Storage, scope blob.Scope, prefix string) ([]core.BlobInfo, error) {
	var out []core.BlobInfo
	in := blob.ListInput{Prefix: prefix}
	for {
		page, err := store.List(ctx, scope, in)
		if err != nil {
			return nil, err
		}
		out = append(out, page.Blobs...)
		if page.NextToken == "" {
			return out, nil
		}
		in.ContinuationToken = page.NextToken
	}
}
