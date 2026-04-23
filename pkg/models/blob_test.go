package models

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superplanehq/superplane/pkg/database"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func Test__Blob(t *testing.T) {
	require.NoError(t, database.TruncateTables())

	steps := blobTestSteps{t: t}
	steps.createOrg()
	steps.createCanvas()
	steps.createNode()

	t.Run("CreatePendingBlob creates a pending row", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		b := steps.createPendingCanvasBlob(tx, "configs/a.yaml")
		require.Equal(t, BlobStatusPending, b.Status)
		require.NotEqual(t, uuid.Nil, b.ID)
	})

	t.Run("FindBlobByPath ignores pending rows", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		steps.createPendingCanvasBlob(tx, "only-pending.txt")

		_, err := FindBlobByPathInTransaction(tx, steps.orgID, BlobScopeCanvas,
			&steps.canvasID, nil, nil, "only-pending.txt")
		require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	})

	t.Run("MarkBlobReady flips status, size, and makes the blob findable", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		b := steps.createPendingCanvasBlob(tx, "ready-me.txt")
		require.NoError(t, MarkBlobReady(tx, b.ID, 42))

		found, err := FindBlobByPathInTransaction(tx, steps.orgID, BlobScopeCanvas,
			&steps.canvasID, nil, nil, "ready-me.txt")
		require.NoError(t, err)
		require.Equal(t, BlobStatusReady, found.Status)
		require.Equal(t, int64(42), found.SizeBytes)
	})

	t.Run("MarkBlobReady replaces the prior ready blob at the same path", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		first := steps.createPendingCanvasBlob(tx, "overwrite.txt")
		require.NoError(t, MarkBlobReady(tx, first.ID, 10))

		second := steps.createPendingCanvasBlob(tx, "overwrite.txt")
		require.NoError(t, MarkBlobReady(tx, second.ID, 20))

		found, err := FindBlobByPathInTransaction(tx, steps.orgID, BlobScopeCanvas,
			&steps.canvasID, nil, nil, "overwrite.txt")
		require.NoError(t, err)
		require.Equal(t, second.ID, found.ID)
		require.Equal(t, int64(20), found.SizeBytes)

		_, err = FindBlobInTransaction(tx, first.ID)
		require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	})

	t.Run("Concurrent pending uploads at the same path both coexist", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		a := steps.createPendingCanvasBlob(tx, "concurrent.txt")
		b := steps.createPendingCanvasBlob(tx, "concurrent.txt")
		require.NotEqual(t, a.ID, b.ID)
	})

	t.Run("ListBlobsByScope returns only ready rows, newest first", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		now := time.Now()

		earlier := steps.createPendingCanvasBlob(tx, "a.txt")
		require.NoError(t, tx.Model(&Blob{}).
			Where("id = ?", earlier.ID).
			Update("created_at", now.Add(-time.Hour)).Error)
		require.NoError(t, MarkBlobReady(tx, earlier.ID, 1))

		later := steps.createPendingCanvasBlob(tx, "b.txt")
		require.NoError(t, tx.Model(&Blob{}).Where("id = ?", later.ID).Update("created_at", now).Error)
		require.NoError(t, MarkBlobReady(tx, later.ID, 2))

		// A pending row should not show up.
		steps.createPendingCanvasBlob(tx, "c-pending.txt")

		out, err := ListBlobsByScopeInTransaction(tx, steps.orgID, BlobScopeCanvas,
			&steps.canvasID, nil, nil, 0, nil)
		require.NoError(t, err)
		require.Len(t, out, 2)
		require.Equal(t, later.ID, out[0].ID)
		require.Equal(t, earlier.ID, out[1].ID)
	})

	t.Run("ListBlobsByScope respects limit and before cursor", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		now := time.Now()
		makeReady := func(path string, at time.Time) *Blob {
			b := steps.createPendingCanvasBlob(tx, path)
			require.NoError(t, tx.Model(&Blob{}).Where("id = ?", b.ID).Update("created_at", at).Error)
			require.NoError(t, MarkBlobReady(tx, b.ID, 1))
			return b
		}
		makeReady("a.txt", now.Add(-3*time.Hour))
		makeReady("b.txt", now.Add(-2*time.Hour))
		makeReady("c.txt", now.Add(-1*time.Hour))

		out, err := ListBlobsByScopeInTransaction(tx, steps.orgID, BlobScopeCanvas,
			&steps.canvasID, nil, nil, 2, nil)
		require.NoError(t, err)
		require.Len(t, out, 2)

		cutoff := now.Add(-90 * time.Minute)
		out, err = ListBlobsByScopeInTransaction(tx, steps.orgID, BlobScopeCanvas,
			&steps.canvasID, nil, nil, 0, &cutoff)
		require.NoError(t, err)
		require.Len(t, out, 2) // only a.txt and b.txt predate the cutoff
	})

	t.Run("ListBlobsByScope isolates scopes", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		canvasBlob := steps.createPendingCanvasBlob(tx, "same-path.txt")
		require.NoError(t, MarkBlobReady(tx, canvasBlob.ID, 1))

		nodeBlob := steps.createPendingNodeBlob(tx, "same-path.txt")
		require.NoError(t, MarkBlobReady(tx, nodeBlob.ID, 2))

		canvasList, err := ListBlobsByScopeInTransaction(tx, steps.orgID, BlobScopeCanvas,
			&steps.canvasID, nil, nil, 0, nil)
		require.NoError(t, err)
		require.Len(t, canvasList, 1)
		require.Equal(t, canvasBlob.ID, canvasList[0].ID)

		nodeList, err := ListBlobsByScopeInTransaction(tx, steps.orgID, BlobScopeNode,
			&steps.canvasID, &steps.nodeID, nil, 0, nil)
		require.NoError(t, err)
		require.Len(t, nodeList, 1)
		require.Equal(t, nodeBlob.ID, nodeList[0].ID)
	})

	t.Run("DeleteBlob removes the row", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		b := steps.createPendingCanvasBlob(tx, "doomed.txt")
		require.NoError(t, MarkBlobReady(tx, b.ID, 1))

		require.NoError(t, DeleteBlobInTransaction(tx, b.ID))

		_, err := FindBlobInTransaction(tx, b.ID)
		require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	})

	t.Run("CHECK constraint rejects scope/ID mismatches", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		// Canvas scope without canvas_id must fail.
		_, err := CreatePendingBlob(tx, CreatePendingBlobInput{
			OrganizationID: steps.orgID,
			ScopeType:      BlobScopeCanvas,
			Path:           "bad.txt",
			ObjectKey:      uuid.NewString(),
		})
		require.Error(t, err)
		require.True(t, isCheckViolation(err), "expected CHECK violation, got %v", err)
	})

	t.Run("CHECK constraint rejects unknown scope_type", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		_, err := CreatePendingBlob(tx, CreatePendingBlobInput{
			OrganizationID: steps.orgID,
			ScopeType:      "workspace",
			Path:           "bad.txt",
			ObjectKey:      uuid.NewString(),
		})
		require.Error(t, err)
		require.True(t, isCheckViolation(err), "expected CHECK violation, got %v", err)
	})

	t.Run("object_key uniqueness is enforced", func(t *testing.T) {
		tx := database.Conn().Begin()
		defer tx.Rollback()

		key := "blobs/" + steps.orgID.String() + "/canvas/" + steps.canvasID.String() + "/dup.txt"
		_, err := CreatePendingBlob(tx, CreatePendingBlobInput{
			OrganizationID: steps.orgID,
			ScopeType:      BlobScopeCanvas,
			CanvasID:       &steps.canvasID,
			Path:           "dup.txt",
			ObjectKey:      key,
		})
		require.NoError(t, err)

		_, err = CreatePendingBlob(tx, CreatePendingBlobInput{
			OrganizationID: steps.orgID,
			ScopeType:      BlobScopeCanvas,
			CanvasID:       &steps.canvasID,
			Path:           "dup-2.txt",
			ObjectKey:      key,
		})
		require.Error(t, err)
		require.True(t, isUniqueViolation(err), "expected unique violation, got %v", err)
	})

	t.Run("Cascade delete from organization removes blobs", func(t *testing.T) {
		require.NoError(t, database.TruncateTables())
		s := blobTestSteps{t: t}
		s.createOrg()
		s.createCanvas()

		tx := database.Conn().Begin()
		b := s.createPendingCanvasBlob(tx, "persist.txt")
		require.NoError(t, MarkBlobReady(tx, b.ID, 1))
		require.NoError(t, tx.Commit().Error)

		// Hard-delete so FK ON DELETE CASCADE fires (Organization has soft-delete by default).
		require.NoError(t, database.Conn().Unscoped().Delete(&Organization{ID: s.orgID}).Error)

		_, err := FindBlob(b.ID)
		require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	})
}

type blobTestSteps struct {
	t *testing.T

	orgID    uuid.UUID
	canvasID uuid.UUID
	nodeID   string
}

func (s *blobTestSteps) createOrg() {
	now := time.Now()
	org := Organization{
		ID:        uuid.New(),
		Name:      "org-" + uuid.NewString()[:8],
		CreatedAt: &now,
		UpdatedAt: &now,
	}
	require.NoError(s.t, database.Conn().Create(&org).Error)
	s.orgID = org.ID
}

func (s *blobTestSteps) createCanvas() {
	now := time.Now()
	liveVersionID := uuid.New()
	c := &Canvas{
		OrganizationID: s.orgID,
		LiveVersionID:  &liveVersionID,
		Name:           "canvas-" + uuid.NewString()[:8],
		CreatedAt:      &now,
		UpdatedAt:      &now,
	}
	require.NoError(s.t, database.Conn().Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(c).Error; err != nil {
			return err
		}
		return tx.Create(&CanvasVersion{
			ID:          liveVersionID,
			WorkflowID:  c.ID,
			State:       CanvasVersionStatePublished,
			PublishedAt: &now,
			Nodes:       datatypes.NewJSONSlice([]Node{}),
			Edges:       datatypes.NewJSONSlice([]Edge{}),
			CreatedAt:   &now,
			UpdatedAt:   &now,
		}).Error
	}))
	s.canvasID = c.ID
}

func (s *blobTestSteps) createNode() {
	node := &CanvasNode{
		WorkflowID: s.canvasID,
		NodeID:     "node-" + uuid.NewString()[:8],
	}
	require.NoError(s.t, database.Conn().Create(node).Error)
	s.nodeID = node.NodeID
}

func (s *blobTestSteps) createPendingCanvasBlob(tx *gorm.DB, path string) *Blob {
	b, err := CreatePendingBlob(tx, CreatePendingBlobInput{
		OrganizationID: s.orgID,
		ScopeType:      BlobScopeCanvas,
		CanvasID:       &s.canvasID,
		Path:           path,
		ObjectKey:      uuid.NewString(),
	})
	require.NoError(s.t, err)
	return b
}

func (s *blobTestSteps) createPendingNodeBlob(tx *gorm.DB, path string) *Blob {
	b, err := CreatePendingBlob(tx, CreatePendingBlobInput{
		OrganizationID: s.orgID,
		ScopeType:      BlobScopeNode,
		CanvasID:       &s.canvasID,
		NodeID:         &s.nodeID,
		Path:           path,
		ObjectKey:      uuid.NewString(),
	})
	require.NoError(s.t, err)
	return b
}

// isCheckViolation reports whether err is a Postgres CHECK violation
// by matching on the pq error text — avoids a direct lib/pq dep.
func isCheckViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "violates check constraint")
}

// isUniqueViolation reports whether err is a Postgres unique violation.
func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "violates unique constraint")
}
