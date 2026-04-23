package blob

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const maxPathBytes = 1024

// validatePath enforces the blob-path rules: non-empty, valid UTF-8,
// no null bytes, no leading/trailing or consecutive slashes, and no
// "."/".." segments; capped at maxPathBytes.
//
// These rules exist to guarantee that object-key generation (see
// objectKey below) always produces a stable, traversal-free key.
func validatePath(p string) error {
	if p == "" {
		return fmt.Errorf("%w: path is empty", ErrInvalidPath)
	}
	if len(p) > maxPathBytes {
		return fmt.Errorf("%w: path exceeds %d bytes", ErrInvalidPath, maxPathBytes)
	}
	if !utf8.ValidString(p) {
		return fmt.Errorf("%w: path is not valid UTF-8", ErrInvalidPath)
	}
	if strings.ContainsRune(p, 0x00) {
		return fmt.Errorf("%w: path contains null byte", ErrInvalidPath)
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("%w: path must not start with /", ErrInvalidPath)
	}
	if strings.HasSuffix(p, "/") {
		return fmt.Errorf("%w: path must not end with /", ErrInvalidPath)
	}
	if strings.Contains(p, "//") {
		return fmt.Errorf("%w: path must not contain consecutive slashes", ErrInvalidPath)
	}
	for seg := range strings.SplitSeq(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("%w: path contains invalid segment %q", ErrInvalidPath, seg)
		}
	}
	return nil
}

// validateScope enforces that the ID fields set on Scope match
// the requirements of its Type. Returning ErrInvalidScope here is
// how the package refuses to generate a key like blobs/canvas//foo.
func validateScope(s Scope) error {
	if s.OrganizationID == "" {
		return fmt.Errorf("%w: organization_id is required", ErrInvalidScope)
	}
	switch s.Type {
	case ScopeOrganization:
		return nil
	case ScopeCanvas:
		if s.CanvasID == "" {
			return fmt.Errorf("%w: canvas_id is required for canvas scope", ErrInvalidScope)
		}
		return nil
	case ScopeNode:
		if s.CanvasID == "" {
			return fmt.Errorf("%w: canvas_id is required for node scope", ErrInvalidScope)
		}
		if s.NodeID == "" {
			return fmt.Errorf("%w: node_id is required for node scope", ErrInvalidScope)
		}
		return nil
	case ScopeExecution:
		if s.ExecutionID == "" {
			return fmt.Errorf("%w: execution_id is required for execution scope", ErrInvalidScope)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown scope type %q", ErrInvalidScope, s.Type)
	}
}

// scopePrefix returns the key prefix (no trailing slash) for a
// scope, e.g. "blobs/org-1/canvas/cvs-1". The org ID sits at the
// top level so that a single "blobs/{org-id}/" prefix covers every
// blob a tenant owns — useful for org-deletion, per-tenant audit,
// and quota accounting. It assumes the scope has already been
// validated.
func scopePrefix(s Scope) string {
	switch s.Type {
	case ScopeOrganization:
		return fmt.Sprintf("blobs/%s/org", s.OrganizationID)
	case ScopeCanvas:
		return fmt.Sprintf("blobs/%s/canvas/%s", s.OrganizationID, s.CanvasID)
	case ScopeNode:
		return fmt.Sprintf("blobs/%s/node/%s/%s", s.OrganizationID, s.CanvasID, s.NodeID)
	case ScopeExecution:
		return fmt.Sprintf("blobs/%s/execution/%s", s.OrganizationID, s.ExecutionID)
	}
	// Unreachable if the scope has been validated.
	return ""
}

// objectKey builds the canonical bucket key for (scope, path) after
// validating both. Callers outside this package must never build
// keys themselves — that would re-introduce the PR #3824 gap where
// components could target arbitrary keys.
func objectKey(s Scope, path string) (string, error) {
	if err := validateScope(s); err != nil {
		return "", err
	}
	if err := validatePath(path); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s", scopePrefix(s), path), nil
}
