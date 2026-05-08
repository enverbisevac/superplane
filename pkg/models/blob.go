package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/database"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Blob scope types. Must stay in sync with the blobs_scope_type_check
// constraint and with pkg/blob.ScopeType values.
const (
	BlobScopeOrganization = "org"
	BlobScopeCanvas       = "canvas"
	BlobScopeNode         = "node"
	BlobScopeExecution    = "execution"
)

// Blob status values. Must stay in sync with blobs_status_check.
const (
	BlobStatusPending = "pending"
	BlobStatusReady   = "ready"
)

// Blob is a row in the blobs table. `pending` rows represent in-flight
// presigned-upload attempts; `ready` rows are what callers see.
type Blob struct {
	ID             uuid.UUID `gorm:"primaryKey;default:gen_random_uuid()"`
	OrganizationID uuid.UUID
	ScopeType      string
	CanvasID       *uuid.UUID
	NodeID         *string
	ExecutionID    *uuid.UUID
	Path           string
	ObjectKey      string
	SizeBytes      int64
	ContentType    *string
	Status         string
	CreatedBy      *uuid.UUID
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (*Blob) TableName() string {
	return "blobs"
}

// BlobScope converts the row's scope + ID fields into a pkg/blob.Scope.
// Shared by the gRPC actions and the public stream handlers so the
// mapping lives in exactly one place.
func (b *Blob) BlobScope() blob.Scope {
	s := blob.Scope{
		Type:           blob.ScopeType(b.ScopeType),
		OrganizationID: b.OrganizationID.String(),
	}
	if b.CanvasID != nil {
		s.CanvasID = b.CanvasID.String()
	}
	if b.NodeID != nil {
		s.NodeID = *b.NodeID
	}
	if b.ExecutionID != nil {
		s.ExecutionID = b.ExecutionID.String()
	}
	return s
}

// CreatePendingBlobInput is the request payload for CreatePendingBlob.
// The caller is responsible for computing object_key from (scope, path)
// in the blob package — the model layer trusts it as-is.
type CreatePendingBlobInput struct {
	OrganizationID uuid.UUID
	ScopeType      string
	CanvasID       *uuid.UUID
	NodeID         *string
	ExecutionID    *uuid.UUID
	Path           string
	ObjectKey      string
	ContentType    *string
	CreatedBy      *uuid.UUID
}

func FindBlob(id uuid.UUID) (*Blob, error) {
	return FindBlobInTransaction(database.Conn(), id)
}

func FindBlobInTransaction(tx *gorm.DB, id uuid.UUID) (*Blob, error) {
	var blob Blob
	err := tx.Where("id = ?", id).First(&blob).Error
	if err != nil {
		return nil, err
	}
	return &blob, nil
}

// FindBlobByPath returns the ready blob at (org, scope, path). Pending
// rows are invisible to this lookup — they only surface via FindBlob(id)
// during the two-phase upload.
func FindBlobByPath(
	orgID uuid.UUID,
	scope string,
	canvasID *uuid.UUID,
	nodeID *string,
	executionID *uuid.UUID,
	path string,
) (*Blob, error) {
	return FindBlobByPathInTransaction(
		database.Conn(),
		orgID,
		scope,
		canvasID,
		nodeID,
		executionID,
		path,
	)
}

func FindBlobByPathInTransaction(
	tx *gorm.DB,
	orgID uuid.UUID,
	scope string,
	canvasID *uuid.UUID,
	nodeID *string,
	executionID *uuid.UUID,
	path string,
) (*Blob, error) {
	var blob Blob
	query := tx.
		Where("organization_id = ?", orgID).
		Where("scope_type = ?", scope).
		Where("status = ?", BlobStatusReady).
		Where("path = ?", path)

	query = applyScopeIDFilters(query, scope, canvasID, nodeID, executionID)

	err := query.First(&blob).Error
	if err != nil {
		return nil, err
	}
	return &blob, nil
}

// ListBlobsByScope returns ready blobs in a scope, newest first, with
// optional limit and `before` cursor (created_at strict upper bound).
func ListBlobsByScope(
	orgID uuid.UUID,
	scope string,
	canvasID *uuid.UUID,
	nodeID *string,
	executionID *uuid.UUID,
	limit int,
	before *time.Time,
) ([]Blob, error) {
	return ListBlobsByScopeInTransaction(
		database.Conn(),
		orgID,
		scope,
		canvasID,
		nodeID,
		executionID,
		limit,
		before,
	)
}

func ListBlobsByScopeInTransaction(
	tx *gorm.DB,
	orgID uuid.UUID,
	scope string,
	canvasID *uuid.UUID,
	nodeID *string,
	executionID *uuid.UUID,
	limit int,
	before *time.Time,
) ([]Blob, error) {
	var blobs []Blob
	query := tx.
		Where("organization_id = ?", orgID).
		Where("scope_type = ?", scope).
		Where("status = ?", BlobStatusReady)

	query = applyScopeIDFilters(query, scope, canvasID, nodeID, executionID)

	if before != nil {
		query = query.Where("created_at < ?", before)
	}
	if limit > 0 {
		query = query.Limit(limit)
	}

	err := query.Order("created_at DESC").Find(&blobs).Error
	if err != nil {
		return nil, err
	}
	return blobs, nil
}

// applyScopeIDFilters attaches the ID-column equality filters implied
// by the scope type. It assumes the caller has validated that required
// pointers are non-nil for the scope — mirroring the DB's scope_ids_check.
func applyScopeIDFilters(
	query *gorm.DB,
	scope string,
	canvasID *uuid.UUID,
	nodeID *string,
	executionID *uuid.UUID,
) *gorm.DB {
	switch scope {
	case BlobScopeCanvas:
		return query.Where("canvas_id = ?", canvasID)
	case BlobScopeNode:
		return query.Where("canvas_id = ?", canvasID).Where("node_id = ?", nodeID)
	case BlobScopeExecution:
		return query.Where("execution_id = ?", executionID)
	}
	return query
}

// CreatePendingBlob inserts a new row with status='pending'. The row
// is visible only via FindBlob(id) until MarkBlobReady flips it.
func CreatePendingBlob(tx *gorm.DB, input CreatePendingBlobInput) (*Blob, error) {
	now := time.Now()
	blob := Blob{
		OrganizationID: input.OrganizationID,
		ScopeType:      input.ScopeType,
		CanvasID:       input.CanvasID,
		NodeID:         input.NodeID,
		ExecutionID:    input.ExecutionID,
		Path:           input.Path,
		ObjectKey:      input.ObjectKey,
		ContentType:    input.ContentType,
		Status:         BlobStatusPending,
		CreatedBy:      input.CreatedBy,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	err := tx.Clauses(clause.Returning{}).Create(&blob).Error
	if err != nil {
		return nil, err
	}
	return &blob, nil
}

// MarkBlobReady flips a pending blob to ready, records its final size,
// and deletes any previously-ready blob at the same (scope, path) in
// the same transaction. Must be called inside a tx.
func MarkBlobReady(tx *gorm.DB, id uuid.UUID, sizeBytes int64) error {
	pending, err := FindBlobInTransaction(tx, id)
	if err != nil {
		return err
	}

	// Delete any stale ready row at the same (scope, path). Pending
	// rows are never matched by this query because of the status
	// filter. Keep this ordered before the UPDATE so the unique
	// partial index on ready rows is never violated.
	delQuery := tx.
		Where("id <> ?", pending.ID).
		Where("organization_id = ?", pending.OrganizationID).
		Where("scope_type = ?", pending.ScopeType).
		Where("status = ?", BlobStatusReady).
		Where("path = ?", pending.Path)

	delQuery = applyScopeIDFilters(
		delQuery,
		pending.ScopeType,
		pending.CanvasID,
		pending.NodeID,
		pending.ExecutionID,
	)

	if err := delQuery.Delete(&Blob{}).Error; err != nil {
		return err
	}

	now := time.Now()
	return tx.Model(&Blob{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"status":     BlobStatusReady,
			"size_bytes": sizeBytes,
			"updated_at": now,
		}).Error
}

func DeleteBlob(id uuid.UUID) error {
	return DeleteBlobInTransaction(database.Conn(), id)
}

func DeleteBlobInTransaction(tx *gorm.DB, id uuid.UUID) error {
	return tx.Where("id = ?", id).Delete(&Blob{}).Error
}
