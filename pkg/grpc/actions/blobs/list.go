package blobs

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/superplanehq/superplane/pkg/authentication"
	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/models"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
)

const (
	defaultListPageSize = 50
	maxListPageSize     = 200
)

// ListBlobs returns READY blobs in a scope, newest first. The
// storage argument is unused in this PR (prefix filtering is applied
// at the DB level; bucket-level listing is a future optimization)
// but is accepted so the action signature is uniform with the rest
// of the package.
func ListBlobs(
	ctx context.Context,
	_ blob.Storage,
	orgID string,
	req *pb.ListBlobsRequest,
) (*pb.ListBlobsResponse, error) {
	if _, userIsSet := authentication.GetUserIdFromMetadata(ctx); !userIsSet {
		return nil, status.Error(codes.Unauthenticated, "user not authenticated")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	resolved, err := resolveAndValidateScope(ctx, orgID, req.Scope)
	if err != nil {
		return nil, err
	}

	pageSize := int(req.PageSize)
	if pageSize <= 0 {
		pageSize = defaultListPageSize
	}
	if pageSize > maxListPageSize {
		pageSize = maxListPageSize
	}

	var before *time.Time
	if req.PageToken != "" {
		t, err := decodePageToken(req.PageToken)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid page_token")
		}
		before = &t
	}

	rows, err := models.ListBlobsByScope(
		mustParseUUID(orgID),
		resolved.ModelType,
		resolved.CanvasID,
		resolved.NodeID,
		resolved.ExecutionID,
		pageSize+1, // fetch one extra to determine if there's a next page
		before,
	)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to list blobs: %v", err))
	}

	// Apply prefix filter at the action layer (cheap — already
	// filtered to a single scope by the DB indexes).
	if prefix := strings.TrimSpace(req.Prefix); prefix != "" {
		filtered := rows[:0]
		for _, r := range rows {
			if strings.HasPrefix(r.Path, prefix) {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}

	out := &pb.ListBlobsResponse{}
	if len(rows) > pageSize {
		// There's a next page. The token is the created_at of the
		// last returned row (exclusive upper bound on the next query).
		out.NextPageToken = encodePageToken(rows[pageSize-1].CreatedAt)
		rows = rows[:pageSize]
	}
	for _, r := range rows {
		out.Blobs = append(out.Blobs, serializeBlob(r))
	}
	return out, nil
}

func encodePageToken(t time.Time) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano)))
}

func decodePageToken(s string) (time.Time, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, string(b))
}
