package blob

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSentinelErrors(t *testing.T) {
	require.Equal(t, "blob not found", ErrBlobNotFound.Error())
	require.Equal(t, "invalid blob path", ErrInvalidPath.Error())
	require.Equal(t, "invalid blob scope", ErrInvalidScope.Error())
	require.Equal(t, "blob presigning not supported by this backend", ErrPresignNotSupported.Error())
	require.Equal(t, "blob storage is not configured", ErrBlobsNotConfigured.Error())

	// Each must be a distinct sentinel, identifiable via errors.Is.
	err := ErrBlobNotFound
	require.True(t, errors.Is(err, ErrBlobNotFound))
	require.False(t, errors.Is(err, ErrInvalidPath))
}

func TestScopeTypeConstants(t *testing.T) {
	require.Equal(t, ScopeType("org"), ScopeOrganization)
	require.Equal(t, ScopeType("canvas"), ScopeCanvas)
	require.Equal(t, ScopeType("node"), ScopeNode)
	require.Equal(t, ScopeType("execution"), ScopeExecution)
}

func newMemStorage(t *testing.T) Storage {
	t.Helper()
	s, err := newStorage(context.Background(), "mem://")
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestStorage_PutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)

	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}
	body := strings.NewReader("hello world")

	require.NoError(t, s.Put(ctx, scope, "greetings/en.txt", body,
		PutOptions{ContentType: "text/plain"}))

	rc, info, err := s.Get(ctx, scope, "greetings/en.txt")
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(got))
	require.Equal(t, "text/plain", info.ContentType)
	require.Equal(t, int64(len("hello world")), info.Size)
	require.Equal(t, "greetings/en.txt", info.Path)
}

func TestStorage_GetNotFound(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}

	_, _, err := s.Get(ctx, scope, "missing.txt")
	require.ErrorIs(t, err, ErrBlobNotFound)
}

func TestStorage_PutRejectsInvalidPath(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}

	err := s.Put(ctx, scope, "../escape.txt", strings.NewReader("x"), PutOptions{})
	require.ErrorIs(t, err, ErrInvalidPath)
}

func TestStorage_PutRejectsInvalidScope(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	bad := Scope{Type: ScopeCanvas, OrganizationID: "org-1"} // no canvas ID

	err := s.Put(ctx, bad, "foo.txt", strings.NewReader("x"), PutOptions{})
	require.ErrorIs(t, err, ErrInvalidScope)
}

func TestStorage_Delete(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}

	require.NoError(t, s.Put(ctx, scope, "doomed.txt", strings.NewReader("x"), PutOptions{}))
	require.NoError(t, s.Delete(ctx, scope, "doomed.txt"))

	_, _, err := s.Get(ctx, scope, "doomed.txt")
	require.ErrorIs(t, err, ErrBlobNotFound)
}

func TestStorage_DeleteMissingReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}

	err := s.Delete(ctx, scope, "nope.txt")
	require.ErrorIs(t, err, ErrBlobNotFound)
}

func TestStorage_ListScopeIsolation(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	a := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-A"}
	b := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-B"}

	require.NoError(t, s.Put(ctx, a, "x.txt", strings.NewReader("1"), PutOptions{}))
	require.NoError(t, s.Put(ctx, a, "y.txt", strings.NewReader("22"), PutOptions{}))
	require.NoError(t, s.Put(ctx, b, "x.txt", strings.NewReader("333"), PutOptions{}))

	listA, err := s.List(ctx, a, ListInput{})
	require.NoError(t, err)
	require.Len(t, listA.Blobs, 2)

	paths := map[string]int64{}
	for _, it := range listA.Blobs {
		paths[it.Path] = it.Size
	}
	require.Equal(t, int64(1), paths["x.txt"])
	require.Equal(t, int64(2), paths["y.txt"])

	listB, err := s.List(ctx, b, ListInput{})
	require.NoError(t, err)
	require.Len(t, listB.Blobs, 1)
	require.Equal(t, "x.txt", listB.Blobs[0].Path)
	require.Equal(t, int64(3), listB.Blobs[0].Size)
}

func TestStorage_ListPrefix(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}

	for _, p := range []string{
		"configs/a.yaml",
		"configs/b.yaml",
		"logs/run-1.txt",
	} {
		require.NoError(t, s.Put(ctx, scope, p, strings.NewReader("x"), PutOptions{}))
	}

	out, err := s.List(ctx, scope, ListInput{Prefix: "configs/"})
	require.NoError(t, err)
	require.Len(t, out.Blobs, 2)

	paths := make([]string, 0, len(out.Blobs))
	for _, b := range out.Blobs {
		paths = append(paths, b.Path)
	}
	require.ElementsMatch(t, []string{"configs/a.yaml", "configs/b.yaml"}, paths)
}

func TestStorage_ListPrefixRejectsAllSlashes(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}

	_, err := s.List(ctx, scope, ListInput{Prefix: "/"})
	require.ErrorIs(t, err, ErrInvalidPath)

	_, err = s.List(ctx, scope, ListInput{Prefix: "//"})
	require.ErrorIs(t, err, ErrInvalidPath)
}

func TestStorage_ListPrefixTrailingSlashNormalized(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}

	require.NoError(t, s.Put(ctx, scope, "configs/a.yaml", strings.NewReader("x"), PutOptions{}))
	require.NoError(t, s.Put(ctx, scope, "configs/b.yaml", strings.NewReader("y"), PutOptions{}))

	// With trailing slash:
	out1, err := s.List(ctx, scope, ListInput{Prefix: "configs/"})
	require.NoError(t, err)
	require.Len(t, out1.Blobs, 2)

	// Without trailing slash: should match the same blobs.
	out2, err := s.List(ctx, scope, ListInput{Prefix: "configs"})
	require.NoError(t, err)
	require.Len(t, out2.Blobs, 2)
}

func TestStorage_ListRejectsContinuationToken(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}

	_, err := s.List(ctx, scope, ListInput{ContinuationToken: "opaque"})
	require.Error(t, err)
}

func TestStorage_PresignNotSupportedOnMem(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}

	_, errPut := s.PresignPut(ctx, scope, "x.txt", PutOptions{}, 15*time.Minute)
	require.ErrorIs(t, errPut, ErrPresignNotSupported)

	_, errGet := s.PresignGet(ctx, scope, "x.txt", 15*time.Minute)
	require.ErrorIs(t, errGet, ErrPresignNotSupported)
}

func TestStorage_PresignRejectsInvalidScope(t *testing.T) {
	ctx := context.Background()
	s := newMemStorage(t)
	bad := Scope{Type: ScopeCanvas, OrganizationID: "org-1"} // no canvas ID

	_, errPut := s.PresignPut(ctx, bad, "x.txt", PutOptions{}, time.Minute)
	require.ErrorIs(t, errPut, ErrInvalidScope)

	_, errGet := s.PresignGet(ctx, bad, "x.txt", time.Minute)
	require.ErrorIs(t, errGet, ErrInvalidScope)
}

func newFileStorage(t *testing.T) Storage {
	t.Helper()
	dir := t.TempDir()
	// fileblob needs create_dir=true so Put creates the scope prefix
	// tree on demand; metadata=skip disables sidecar .attrs files
	// that would otherwise surface in listings.
	url := "file://" + dir + "?create_dir=true&metadata=skip"
	s, err := newStorage(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestFileStorage_PutGetDeleteList(t *testing.T) {
	ctx := context.Background()
	s := newFileStorage(t)
	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}

	require.NoError(t, s.Put(ctx, scope, "a.txt", strings.NewReader("A"), PutOptions{}))
	require.NoError(t, s.Put(ctx, scope, "b.txt", strings.NewReader("BB"), PutOptions{}))

	out, err := s.List(ctx, scope, ListInput{})
	require.NoError(t, err)
	require.Len(t, out.Blobs, 2)

	rc, _, err := s.Get(ctx, scope, "a.txt")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, "A", string(got))

	require.NoError(t, s.Delete(ctx, scope, "a.txt"))

	_, _, err = s.Get(ctx, scope, "a.txt")
	require.ErrorIs(t, err, ErrBlobNotFound)
}

func TestFileStorage_PresignNotSupported(t *testing.T) {
	ctx := context.Background()
	s := newFileStorage(t)
	scope := Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"}

	_, err := s.PresignPut(ctx, scope, "x.txt", PutOptions{}, time.Minute)
	require.ErrorIs(t, err, ErrPresignNotSupported)

	_, err = s.PresignGet(ctx, scope, "x.txt", time.Minute)
	require.ErrorIs(t, err, ErrPresignNotSupported)
}

func TestFileStorage_ScopeIsolation(t *testing.T) {
	ctx := context.Background()
	s := newFileStorage(t)
	a := Scope{Type: ScopeNode, OrganizationID: "org-1", CanvasID: "cvs-1", NodeID: "node-A"}
	b := Scope{Type: ScopeNode, OrganizationID: "org-1", CanvasID: "cvs-1", NodeID: "node-B"}

	require.NoError(t, s.Put(ctx, a, "shared.txt", strings.NewReader("A"), PutOptions{}))
	require.NoError(t, s.Put(ctx, b, "shared.txt", strings.NewReader("B"), PutOptions{}))

	rcA, _, err := s.Get(ctx, a, "shared.txt")
	require.NoError(t, err)
	gotA, err := io.ReadAll(rcA)
	require.NoError(t, err)
	require.NoError(t, rcA.Close())

	rcB, _, err := s.Get(ctx, b, "shared.txt")
	require.NoError(t, err)
	gotB, err := io.ReadAll(rcB)
	require.NoError(t, err)
	require.NoError(t, rcB.Close())

	require.Equal(t, "A", string(gotA))
	require.Equal(t, "B", string(gotB))
}
