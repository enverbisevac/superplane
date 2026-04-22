package blob

import (
	"context"
	"errors"
	"io"
	"time"
)

// Sentinel errors returned by Storage implementations. Callers should
// match on these via errors.Is — backend-specific errors are wrapped.
var (
	// ErrBlobNotFound is returned when a requested blob does not exist.
	ErrBlobNotFound = errors.New("blob not found")

	// ErrInvalidPath is returned when the caller-supplied blob path
	// fails validation (empty, leading slash, traversal, too long).
	ErrInvalidPath = errors.New("invalid blob path")

	// ErrInvalidScope is returned when the Scope is missing IDs
	// required for its ScopeType.
	ErrInvalidScope = errors.New("invalid blob scope")

	// ErrPresignNotSupported is returned by backends that cannot
	// generate presigned URLs (memory, filesystem). Callers should
	// fall back to streaming Put/Get.
	ErrPresignNotSupported = errors.New("blob presigning not supported by this backend")

	// ErrBlobsNotConfigured is returned when storage has not been
	// initialized (nil Storage passed into a higher layer).
	ErrBlobsNotConfigured = errors.New("blob storage is not configured")
)

// ScopeType names one of the four levels blobs can live at.
type ScopeType string

const (
	ScopeOrganization ScopeType = "org"
	ScopeCanvas       ScopeType = "canvas"
	ScopeNode         ScopeType = "node"
	ScopeExecution    ScopeType = "execution"
)

// Scope fully identifies a blob namespace. Fields not required by
// the ScopeType must be empty — see keys.go for validation rules.
type Scope struct {
	Type           ScopeType
	OrganizationID string
	CanvasID       string
	NodeID         string
	ExecutionID    string
}

// PutOptions controls how a blob is written.
type PutOptions struct {
	ContentType string
}

// BlobInfo describes a stored blob without its contents.
type BlobInfo struct {
	Path        string
	Size        int64
	ContentType string
	UpdatedAt   time.Time
}

// ListInput controls pagination and filtering for List. The
// ContinuationToken is opaque to callers — pass back the NextToken
// returned by a prior List call on the same scope.
type ListInput struct {
	Prefix            string
	MaxResults        int
	ContinuationToken string
}

// ListOutput is the paginated result of List.
type ListOutput struct {
	Blobs     []BlobInfo
	NextToken string
}

// PresignedURL is an out-of-band URL a client uses to upload or
// download bytes directly against the underlying bucket, bypassing
// the SuperPlane API. Backends that cannot presign return
// ErrPresignNotSupported.
type PresignedURL struct {
	URL       string
	Method    string            // "PUT" or "GET"
	Headers   map[string]string // headers the client must send
	ExpiresAt time.Time
}

// Storage is the single interface the rest of SuperPlane uses.
type Storage interface {
	Put(ctx context.Context, scope Scope, path string, body io.Reader, opts PutOptions) error
	Get(ctx context.Context, scope Scope, path string) (io.ReadCloser, BlobInfo, error)
	Delete(ctx context.Context, scope Scope, path string) error
	List(ctx context.Context, scope Scope, in ListInput) (*ListOutput, error)

	PresignPut(ctx context.Context, scope Scope, path string, opts PutOptions, ttl time.Duration) (*PresignedURL, error)
	PresignGet(ctx context.Context, scope Scope, path string, ttl time.Duration) (*PresignedURL, error)

	Close() error
}
