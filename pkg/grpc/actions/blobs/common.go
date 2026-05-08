// Package blobs hosts the gRPC action implementations for the
// Superplane Blobs API. This file provides helpers shared across
// actions in the package: scope-type conversion, scope validation
// against the authenticated org, and proto serialization of the
// models.Blob row.
package blobs

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	"github.com/superplanehq/superplane/pkg/authentication"
	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/database"
	"github.com/superplanehq/superplane/pkg/models"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
)

// defaultUploadTTLSeconds is the presigned-URL TTL applied when the
// client does not request one (15 minutes). maxUploadTTLSeconds caps
// what the client may ask for (1 hour) — anything larger is clamped
// down by clampTTL. Both values will become env-driven (BLOB_UPLOAD_TTL_SECONDS
// and friends) once quota tuning lands.
const defaultUploadTTLSeconds int64 = 15 * 60
const maxUploadTTLSeconds int64 = 60 * 60

// scopeTypeToModel maps proto enum -> DB string. Rejects UNSPECIFIED.
func scopeTypeToModel(t pb.BlobScopeType) (string, error) {
	switch t {
	case pb.BlobScopeType_BLOB_SCOPE_ORGANIZATION:
		return models.BlobScopeOrganization, nil
	case pb.BlobScopeType_BLOB_SCOPE_CANVAS:
		return models.BlobScopeCanvas, nil
	case pb.BlobScopeType_BLOB_SCOPE_NODE:
		return models.BlobScopeNode, nil
	case pb.BlobScopeType_BLOB_SCOPE_EXECUTION:
		return models.BlobScopeExecution, nil
	}
	return "", fmt.Errorf("unspecified or unknown scope type")
}

// scopeTypeToBlobPackage maps proto enum -> pkg/blob.ScopeType.
// Thin wrapper around scopeTypeToModel; the string values are
// identical across both layers by design.
func scopeTypeToBlobPackage(t pb.BlobScopeType) (blob.ScopeType, error) {
	s, err := scopeTypeToModel(t)
	if err != nil {
		return "", err
	}
	return blob.ScopeType(s), nil
}

// scopeTypeFromModel is the inverse of scopeTypeToModel for
// serialization. Returns BLOB_SCOPE_UNSPECIFIED for unknown input so
// stale/garbage rows do not panic the caller.
func scopeTypeFromModel(s string) pb.BlobScopeType {
	switch s {
	case models.BlobScopeOrganization:
		return pb.BlobScopeType_BLOB_SCOPE_ORGANIZATION
	case models.BlobScopeCanvas:
		return pb.BlobScopeType_BLOB_SCOPE_CANVAS
	case models.BlobScopeNode:
		return pb.BlobScopeType_BLOB_SCOPE_NODE
	case models.BlobScopeExecution:
		return pb.BlobScopeType_BLOB_SCOPE_EXECUTION
	}
	return pb.BlobScopeType_BLOB_SCOPE_UNSPECIFIED
}

// resolvedScope is the sanitized, server-trusted form of a scope.
// All IDs here are parsed UUIDs (or a string NodeID) - never trust
// the raw request fields after this helper returns.
type resolvedScope struct {
	ModelType   string
	BlobScope   blob.Scope
	CanvasID    *uuid.UUID
	NodeID      *string
	ExecutionID *uuid.UUID
}

// resolveAndValidateScope takes the authenticated organization ID
// (from the authorization interceptor) + the request-supplied
// BlobScope, and returns a sanitized resolvedScope. It enforces:
//
//   - scope.organization_id, if set, must equal orgID from the auth
//     context (reject attempts to pass a different org_id).
//   - required ID fields per scope type are present and parse.
//   - for canvas/node scopes, the canvas exists and belongs to orgID.
//   - for execution scopes, the execution exists and its canvas
//     belongs to orgID (see verifyExecutionInOrg).
//
// Returns codes.InvalidArgument for malformed input and codes.NotFound
// (not PermissionDenied - mirror the existing pattern in
// pkg/grpc/actions/secrets) when IDs do not belong to the org.
func resolveAndValidateScope(ctx context.Context, orgID string, scope *pb.BlobScope) (*resolvedScope, error) {
	_ = ctx
	if scope == nil {
		return nil, status.Error(codes.InvalidArgument, "missing scope")
	}
	modelType, err := scopeTypeToModel(scope.Type)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		return nil, status.Error(codes.Internal, "malformed organization ID in context")
	}
	// The client must either omit organization_id or pass the org it
	// is authenticated against. Passing a different org is a client
	// bug and is rejected rather than silently overridden.
	if scope.OrganizationId != "" && scope.OrganizationId != orgID {
		return nil, status.Error(
			codes.InvalidArgument,
			"scope.organization_id does not match authenticated organization",
		)
	}

	out := &resolvedScope{
		ModelType: modelType,
		BlobScope: blob.Scope{
			Type:           blob.ScopeType(modelType),
			OrganizationID: orgID,
		},
	}

	switch modelType {
	case models.BlobScopeOrganization:
		return out, nil

	case models.BlobScopeCanvas:
		canvasUUID, err := uuid.Parse(scope.CanvasId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid scope.canvas_id")
		}
		if _, err := findCanvasInOrg(orgUUID, canvasUUID); err != nil {
			return nil, err
		}
		out.CanvasID = &canvasUUID
		out.BlobScope.CanvasID = scope.CanvasId
		return out, nil

	case models.BlobScopeNode:
		canvasUUID, err := uuid.Parse(scope.CanvasId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid scope.canvas_id")
		}
		if _, err := findCanvasInOrg(orgUUID, canvasUUID); err != nil {
			return nil, err
		}
		if scope.NodeId == "" {
			return nil, status.Error(codes.InvalidArgument, "missing scope.node_id")
		}
		nodeID := scope.NodeId
		out.CanvasID = &canvasUUID
		out.NodeID = &nodeID
		out.BlobScope.CanvasID = scope.CanvasId
		out.BlobScope.NodeID = nodeID
		return out, nil

	case models.BlobScopeExecution:
		executionUUID, err := uuid.Parse(scope.ExecutionId)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid scope.execution_id")
		}
		if err := verifyExecutionInOrg(orgUUID, executionUUID); err != nil {
			return nil, err
		}
		out.ExecutionID = &executionUUID
		out.BlobScope.ExecutionID = scope.ExecutionId
		return out, nil
	}
	return nil, status.Error(codes.InvalidArgument, "unknown scope type")
}

// findCanvasInOrg returns the canvas if it exists and belongs to
// orgID. Wraps models.FindCanvas(orgID, id), which already scopes
// the lookup by organization_id - so a miss can mean either the
// canvas does not exist or it belongs to a different org. Both are
// surfaced as codes.NotFound to avoid leaking the existence of
// sibling-org resources (same pattern used by other actions).
func findCanvasInOrg(orgID, canvasID uuid.UUID) (*models.Canvas, error) {
	canvas, err := models.FindCanvas(orgID, canvasID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "canvas not found")
		}
		return nil, status.Error(codes.Internal, "failed to load canvas")
	}
	return canvas, nil
}

// verifyExecutionInOrg confirms that an execution's canvas belongs
// to the authenticated org. It joins workflow_node_executions to
// workflows (the underlying table for models.Canvas) via workflow_id
// and filters on workflows.organization_id. Returns codes.NotFound if
// no row matches (which covers both "execution does not exist" and
// "execution belongs to a sibling org") so the helper does not leak
// the existence of cross-org resources.
func verifyExecutionInOrg(orgID, executionID uuid.UUID) error {
	var count int64
	err := database.Conn().
		Table("workflow_node_executions").
		Joins("JOIN workflows ON workflows.id = workflow_node_executions.workflow_id").
		Where("workflow_node_executions.id = ?", executionID).
		Where("workflows.organization_id = ?", orgID).
		Count(&count).Error
	if err != nil {
		return status.Error(codes.Internal, "failed to verify execution ownership")
	}
	if count == 0 {
		return status.Error(codes.NotFound, "execution not found")
	}
	return nil
}

// verifyBlobBelongsToOrg is used by operations that take a blob ID
// directly (Describe/Download/Complete/Delete). Loads the row and
// checks organization_id. Returns codes.NotFound on any miss so we
// do not leak the existence of sibling-org blobs.
func verifyBlobBelongsToOrg(orgID string, id uuid.UUID) (*models.Blob, error) {
	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		return nil, status.Error(codes.Internal, "malformed organization ID in context")
	}
	b, err := models.FindBlob(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "blob not found")
		}
		return nil, status.Error(codes.Internal, "failed to load blob")
	}
	if b.OrganizationID != orgUUID {
		return nil, status.Error(codes.NotFound, "blob not found")
	}
	return b, nil
}

// serializeBlob converts a DB row into the proto wire format.
// Nil-safe against optional columns (canvas_id, node_id,
// execution_id, content_type, created_by).
func serializeBlob(row models.Blob) *pb.Blob {
	out := &pb.Blob{
		Id: row.ID.String(),
		Scope: &pb.BlobScope{
			Type:           scopeTypeFromModel(row.ScopeType),
			OrganizationId: row.OrganizationID.String(),
		},
		Path:      row.Path,
		SizeBytes: row.SizeBytes,
		Status:    statusFromModel(row.Status),
		CreatedAt: timestamppb.New(row.CreatedAt),
		UpdatedAt: timestamppb.New(row.UpdatedAt),
	}
	if row.CanvasID != nil {
		out.Scope.CanvasId = row.CanvasID.String()
	}
	if row.NodeID != nil {
		out.Scope.NodeId = *row.NodeID
	}
	if row.ExecutionID != nil {
		out.Scope.ExecutionId = row.ExecutionID.String()
	}
	if row.ContentType != nil {
		out.ContentType = *row.ContentType
	}
	if row.CreatedBy != nil {
		out.CreatedByUserId = row.CreatedBy.String()
	}
	return out
}

// statusFromModel maps the DB status string to the proto enum.
// Unknown values become STATUS_UNSPECIFIED rather than panicking.
func statusFromModel(s string) pb.Blob_Status {
	switch s {
	case models.BlobStatusPending:
		return pb.Blob_STATUS_PENDING
	case models.BlobStatusReady:
		return pb.Blob_STATUS_READY
	}
	return pb.Blob_STATUS_UNSPECIFIED
}

// buildStreamPath returns the relative path for a streaming fallback
// endpoint — either ":stream" (upload) or ":download". The public
// HTTP server mounts handlers at these paths. Shared by StoreBlob
// and DownloadBlob so the URL format lives in one place.
func buildStreamPath(id uuid.UUID, verb string) string {
	return "/api/v1/blobs/" + id.String() + ":" + verb
}

// loadAuthenticatedBlobByID is the shared prelude for the four
// blob-by-id actions (Describe, Download, Complete, Delete): it
// authenticates the caller, parses the request's ID, and verifies
// the row belongs to the authenticated organization. The req
// parameter is the typed proto request; its GetId method is
// nil-safe (generated protos return "" on a nil receiver).
func loadAuthenticatedBlobByID(ctx context.Context, orgID string, req interface{ GetId() string }) (*models.Blob, error) {
	if _, userIsSet := authentication.GetUserIdFromMetadata(ctx); !userIsSet {
		return nil, status.Error(codes.Unauthenticated, "user not authenticated")
	}
	id := req.GetId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "missing blob id")
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid blob id")
	}
	return verifyBlobBelongsToOrg(orgID, parsed)
}

// clampTTL coerces the client-requested TTL (in seconds) to the
// server policy range. 0 or negative -> default; above max -> max.
func clampTTL(requested int64) int64 {
	if requested <= 0 {
		return defaultUploadTTLSeconds
	}
	if requested > maxUploadTTLSeconds {
		return maxUploadTTLSeconds
	}
	return requested
}
