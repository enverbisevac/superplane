package blobs

import (
	"context"

	"github.com/superplanehq/superplane/pkg/blob"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
)

// DescribeBlob returns metadata for a blob by ID. Pending blobs are
// visible via this path (so clients can poll an upload), but the
// typical consumer is UI rendering of a ready blob.
func DescribeBlob(
	ctx context.Context,
	_ blob.Storage,
	orgID string,
	req *pb.DescribeBlobRequest,
) (*pb.DescribeBlobResponse, error) {
	row, err := loadAuthenticatedBlobByID(ctx, orgID, req)
	if err != nil {
		return nil, err
	}
	return &pb.DescribeBlobResponse{Blob: serializeBlob(*row)}, nil
}
