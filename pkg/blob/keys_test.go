package blob

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidatePath_Valid(t *testing.T) {
	cases := []string{
		"a",
		"file.txt",
		"configs/prod.yaml",
		"a/b/c/d.json",
		strings.Repeat("a", 1024), // exactly at limit
	}
	for _, p := range cases {
		t.Run(p, func(t *testing.T) {
			require.NoError(t, validatePath(p))
		})
	}
}

func TestValidatePath_Invalid(t *testing.T) {
	cases := map[string]string{
		"empty":             "",
		"leading-slash":     "/etc/passwd",
		"trailing-slash":    "foo/",
		"double-slash":      "a//b",
		"single-dot":        "./foo",
		"double-dot":        "../foo",
		"double-dot-middle": "a/../b",
		"double-dot-end":    "a/..",
		"null-byte":         "foo\x00bar",
		"too-long":          strings.Repeat("a", 1025),
		"invalid-utf8":      "\xff\xfe",
	}
	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			err := validatePath(p)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidPath)
		})
	}
}

func TestValidateScope_Valid(t *testing.T) {
	cases := map[string]Scope{
		"organization": {Type: ScopeOrganization, OrganizationID: "org-1"},
		"canvas":       {Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"},
		"node":         {Type: ScopeNode, OrganizationID: "org-1", CanvasID: "cvs-1", NodeID: "my-node"},
		"execution":    {Type: ScopeExecution, OrganizationID: "org-1", ExecutionID: "exec-1"},
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, validateScope(s))
		})
	}
}

func TestValidateScope_Invalid(t *testing.T) {
	cases := map[string]Scope{
		"unknown-type":      {Type: ScopeType("other"), OrganizationID: "org-1"},
		"empty-org":         {Type: ScopeOrganization},
		"canvas-no-canvas":  {Type: ScopeCanvas, OrganizationID: "org-1"},
		"canvas-no-org":     {Type: ScopeCanvas, CanvasID: "cvs-1"},
		"node-no-node":      {Type: ScopeNode, OrganizationID: "org-1", CanvasID: "cvs-1"},
		"node-no-canvas":    {Type: ScopeNode, OrganizationID: "org-1", NodeID: "n-1"},
		"execution-no-exec": {Type: ScopeExecution, OrganizationID: "org-1"},
		"execution-no-org":  {Type: ScopeExecution, ExecutionID: "exec-1"},
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			err := validateScope(s)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidScope)
		})
	}
}

func TestObjectKey_Valid(t *testing.T) {
	cases := []struct {
		name     string
		scope    Scope
		path     string
		expected string
	}{
		{
			name:     "organization",
			scope:    Scope{Type: ScopeOrganization, OrganizationID: "org-1"},
			path:     "logos/sp.png",
			expected: "blobs/org-1/org/logos/sp.png",
		},
		{
			name:     "canvas",
			scope:    Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"},
			path:     "configs/prod.yaml",
			expected: "blobs/org-1/canvas/cvs-1/configs/prod.yaml",
		},
		{
			name:     "node",
			scope:    Scope{Type: ScopeNode, OrganizationID: "org-1", CanvasID: "cvs-1", NodeID: "deploy"},
			path:     "input.tgz",
			expected: "blobs/org-1/node/cvs-1/deploy/input.tgz",
		},
		{
			name:     "execution",
			scope:    Scope{Type: ScopeExecution, OrganizationID: "org-1", ExecutionID: "exec-9"},
			path:     "out/log.txt",
			expected: "blobs/org-1/execution/exec-9/out/log.txt",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ObjectKey(c.scope, c.path)
			require.NoError(t, err)
			require.Equal(t, c.expected, got)
		})
	}
}

func TestObjectKey_RejectsBadScope(t *testing.T) {
	_, err := ObjectKey(Scope{Type: ScopeCanvas, OrganizationID: "org-1"}, "foo")
	require.ErrorIs(t, err, ErrInvalidScope)
}

func TestObjectKey_RejectsBadPath(t *testing.T) {
	_, err := ObjectKey(
		Scope{Type: ScopeCanvas, OrganizationID: "org-1", CanvasID: "cvs-1"},
		"../etc/passwd",
	)
	require.ErrorIs(t, err, ErrInvalidPath)
}
