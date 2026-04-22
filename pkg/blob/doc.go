// Package blob is blob storage abstraction.
//
// It wraps gocloud.dev/blob with a small typed API so callers pass a
// Scope (Organization / Canvas / Node / Execution) and a user-facing
// path, rather than raw object keys. Key generation and path
// validation live inside this package and are never trusted from
// external callers.
package blob
