package blob

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewFromEnv_DefaultIsMemory(t *testing.T) {
	t.Setenv("BLOB_BUCKET_URL", "")
	s, err := NewFromEnv(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NotNil(t, s)
}

func TestNewFromEnv_HonorsExplicitURL(t *testing.T) {
	t.Setenv("BLOB_BUCKET_URL", "mem://")
	s, err := NewFromEnv(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NotNil(t, s)
}

func TestNewFromEnv_RejectsBadURL(t *testing.T) {
	t.Setenv("BLOB_BUCKET_URL", "notascheme://nope")
	_, err := NewFromEnv(context.Background())
	require.Error(t, err)
}
