package blob

import (
	"context"
	"os"
)

// EnvBucketURL is the single environment variable that selects the
// backend for all blob storage. Supported URL schemes:
//
//	mem://                              in-memory (default for tests / fresh dev)
//	file:///var/superplane/blobs        local filesystem (no presign)
//	gs://my-bucket                      Google Cloud Storage (SaaS default)
//	s3://my-bucket?region=us-east-1     AWS S3
//	azblob://my-container?account=foo   Azure Blob Storage
//
// Credentials come from the standard cloud-provider env vars
// (GOOGLE_APPLICATION_CREDENTIALS, AWS_*, AZURE_STORAGE_ACCOUNT, …).
const EnvBucketURL = "BLOB_BUCKET_URL"

// defaultBucketURL is used when the env var is unset. The in-memory
// backend is safe for tests and for a fresh dev install where blob
// storage has not yet been provisioned.
const defaultBucketURL = "mem://"

// NewFromEnv returns a Storage configured from BLOB_BUCKET_URL. The
// returned Storage must be Close()d on shutdown.
func NewFromEnv(ctx context.Context) (Storage, error) {
	url := os.Getenv(EnvBucketURL)
	if url == "" {
		url = defaultBucketURL
	}
	return newStorage(ctx, url)
}
