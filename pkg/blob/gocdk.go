package blob

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"gocloud.dev/blob"
	// Blank imports register Go CDK bucket drivers with blob.OpenBucket
	// so a URL like "gs://…" / "s3://…" / "azblob://…" / "file://…" /
	// "mem://" resolves to the matching backend at runtime.
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"

	"gocloud.dev/gcerrors"
)

// storage is the single Storage implementation. It wraps a
// *blob.Bucket and translates scoped operations into key-prefixed
// bucket operations.
type storage struct {
	bucket *blob.Bucket
}

// newStorage opens a gocloud.dev bucket from a URL (e.g.
// "mem://", "file:///var/superplane/blobs", "gs://my-bucket",
// "s3://my-bucket?region=us-east-1").
func newStorage(ctx context.Context, bucketURL string) (*storage, error) {
	b, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("opening blob bucket %q: %w", bucketURL, err)
	}
	return &storage{bucket: b}, nil
}

// Close releases any resources held by the underlying bucket. Safe
// to call on a nil receiver.
func (s *storage) Close() error {
	if s == nil || s.bucket == nil {
		return nil
	}
	return s.bucket.Close()
}

// Put streams body into the bucket under a key derived from scope
// and path. The body is read until EOF.
func (s *storage) Put(ctx context.Context, scope Scope, path string, body io.Reader, opts PutOptions) error {
	key, err := ObjectKey(scope, path)
	if err != nil {
		return err
	}
	w, err := s.bucket.NewWriter(ctx, key, &blob.WriterOptions{
		ContentType: opts.ContentType,
	})
	if err != nil {
		return fmt.Errorf("opening writer for %q: %w", key, err)
	}
	if _, err := io.Copy(w, body); err != nil {
		_ = w.Close()
		return fmt.Errorf("writing %q: %w", key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("closing writer for %q: %w", key, err)
	}
	return nil
}

// Get returns a ReadCloser over the blob's bytes and a BlobInfo
// describing it. The caller must Close the ReadCloser. Returns
// ErrBlobNotFound when the blob does not exist.
func (s *storage) Get(ctx context.Context, scope Scope, path string) (io.ReadCloser, BlobInfo, error) {
	key, err := ObjectKey(scope, path)
	if err != nil {
		return nil, BlobInfo{}, err
	}
	r, err := s.bucket.NewReader(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, BlobInfo{}, ErrBlobNotFound
		}
		return nil, BlobInfo{}, fmt.Errorf("opening reader for %q: %w", key, err)
	}
	info := BlobInfo{
		Path:        path,
		Size:        r.Size(),
		ContentType: r.ContentType(),
		UpdatedAt:   r.ModTime(),
	}
	return r, info, nil
}

// Delete removes a blob. Returns ErrBlobNotFound when the blob does
// not exist — behavior is safe for idempotent cleanup workflows
// that may double-delete.
func (s *storage) Delete(ctx context.Context, scope Scope, path string) error {
	key, err := ObjectKey(scope, path)
	if err != nil {
		return err
	}
	if err := s.bucket.Delete(ctx, key); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return ErrBlobNotFound
		}
		return fmt.Errorf("deleting %q: %w", key, err)
	}
	return nil
}

// List enumerates blobs under a scope. Optional Prefix filters to
// keys beginning with scopePrefix+"/"+Prefix. MaxResults caps the
// returned set (0 = unlimited up to backend's default).
//
// ListInput.ContinuationToken is reserved for future pagination; it
// is currently ignored (a single List call returns everything that
// fits under MaxResults).
func (s *storage) List(ctx context.Context, scope Scope, in ListInput) (*ListOutput, error) {
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	if in.ContinuationToken != "" {
		return nil, fmt.Errorf("pagination not yet supported: continuation token was provided")
	}

	prefix := scopePrefix(scope) + "/"
	if in.Prefix != "" {
		// Accept a trailing slash on the user-supplied prefix even
		// though validatePath rejects it on full blob paths.
		cleaned := strings.TrimRight(in.Prefix, "/")
		if cleaned == "" {
			return nil, fmt.Errorf("%w: prefix must not be all slashes", ErrInvalidPath)
		}
		if err := validatePath(cleaned); err != nil {
			return nil, err
		}
		prefix += cleaned
	}

	it := s.bucket.List(&blob.ListOptions{Prefix: prefix})
	out := &ListOutput{}
	for {
		obj, err := it.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing %q: %w", prefix, err)
		}
		if obj.IsDir {
			continue
		}
		out.Blobs = append(out.Blobs, BlobInfo{
			Path:      stripScopePrefix(obj.Key, scope),
			Size:      obj.Size,
			UpdatedAt: obj.ModTime,
		})
		if in.MaxResults > 0 && len(out.Blobs) >= in.MaxResults {
			break
		}
	}
	return out, nil
}

// stripScopePrefix trims the scope's key prefix off a full bucket
// key, leaving only the user-visible path.
func stripScopePrefix(key string, scope Scope) string {
	return strings.TrimPrefix(key, scopePrefix(scope)+"/")
}

// PresignPut returns a URL the caller uses to upload bytes directly
// into the bucket. Backends that cannot sign URLs (mem, file) return
// ErrPresignNotSupported so the API layer can fall back to streaming.
func (s *storage) PresignPut(ctx context.Context, scope Scope, path string, opts PutOptions, ttl time.Duration) (*PresignedURL, error) {
	key, err := ObjectKey(scope, path)
	if err != nil {
		return nil, err
	}
	signed, err := s.bucket.SignedURL(ctx, key, &blob.SignedURLOptions{
		Method:      http.MethodPut,
		Expiry:      ttl,
		ContentType: opts.ContentType,
	})
	if err != nil {
		if gcerrors.Code(err) == gcerrors.Unimplemented {
			return nil, ErrPresignNotSupported
		}
		return nil, fmt.Errorf("presigning put for %q: %w", key, err)
	}
	result := &PresignedURL{
		URL:       signed,
		Method:    http.MethodPut,
		ExpiresAt: time.Now().Add(ttl),
	}
	if opts.ContentType != "" {
		result.Headers = map[string]string{"Content-Type": opts.ContentType}
	}
	return result, nil
}

// PresignGet returns a URL the caller uses to download bytes
// directly from the bucket. Backends that cannot sign URLs return
// ErrPresignNotSupported.
func (s *storage) PresignGet(ctx context.Context, scope Scope, path string, ttl time.Duration) (*PresignedURL, error) {
	key, err := ObjectKey(scope, path)
	if err != nil {
		return nil, err
	}
	signed, err := s.bucket.SignedURL(ctx, key, &blob.SignedURLOptions{
		Method: http.MethodGet,
		Expiry: ttl,
	})
	if err != nil {
		if gcerrors.Code(err) == gcerrors.Unimplemented {
			return nil, ErrPresignNotSupported
		}
		return nil, fmt.Errorf("presigning get for %q: %w", key, err)
	}
	return &PresignedURL{
		URL:       signed,
		Method:    http.MethodGet,
		ExpiresAt: time.Now().Add(ttl),
	}, nil
}
