package grpc

import (
	"context"

	"github.com/superplanehq/superplane/pkg/authorization"
	"github.com/superplanehq/superplane/pkg/blob"
	"github.com/superplanehq/superplane/pkg/grpc/actions/blobs"
	pb "github.com/superplanehq/superplane/pkg/protos/blobs"
)

// BlobsService implements pb.BlobsServer. It is a thin delegator:
// every method pulls the authenticated organization ID out of the
// context (set by the authorization interceptor from x-organization-id
// metadata) and forwards to the pure action function.
type BlobsService struct {
	pb.UnimplementedBlobsServer
	storage     blob.Storage
	authService authorization.Authorization
}

func NewBlobsService(
	storage blob.Storage,
	authService authorization.Authorization,
) *BlobsService {
	return &BlobsService{
		storage:     storage,
		authService: authService,
	}
}

func (s *BlobsService) StoreBlob(
	ctx context.Context,
	req *pb.StoreBlobRequest,
) (*pb.StoreBlobResponse, error) {
	return blobs.StoreBlob(
		ctx,
		s.storage,
		domainIDFromContext(ctx),
		req,
	)
}

func (s *BlobsService) CompleteStoreBlob(
	ctx context.Context,
	req *pb.CompleteStoreBlobRequest,
) (*pb.CompleteStoreBlobResponse, error) {
	return blobs.CompleteStoreBlob(
		ctx,
		s.storage,
		domainIDFromContext(ctx),
		req,
	)
}

func (s *BlobsService) ListBlobs(
	ctx context.Context,
	req *pb.ListBlobsRequest,
) (*pb.ListBlobsResponse, error) {
	return blobs.ListBlobs(
		ctx,
		s.storage,
		domainIDFromContext(ctx),
		req,
	)
}

func (s *BlobsService) DescribeBlob(
	ctx context.Context,
	req *pb.DescribeBlobRequest,
) (*pb.DescribeBlobResponse, error) {
	return blobs.DescribeBlob(
		ctx,
		s.storage,
		domainIDFromContext(ctx),
		req,
	)
}

func (s *BlobsService) DownloadBlob(
	ctx context.Context,
	req *pb.DownloadBlobRequest,
) (*pb.DownloadBlobResponse, error) {
	return blobs.DownloadBlob(
		ctx,
		s.storage,
		domainIDFromContext(ctx),
		req,
	)
}

func (s *BlobsService) DeleteBlob(
	ctx context.Context,
	req *pb.DeleteBlobRequest,
) (*pb.DeleteBlobResponse, error) {
	return blobs.DeleteBlob(
		ctx,
		s.storage,
		domainIDFromContext(ctx),
		req,
	)
}

// domainIDFromContext pulls the domain (organization) ID placed in
// the context by the authorization interceptor.
func domainIDFromContext(ctx context.Context) string {
	v := ctx.Value(authorization.DomainIdContextKey)
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
