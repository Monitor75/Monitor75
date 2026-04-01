#ifndef STORAGENODESERVICEIMPL_H
#define STORAGENODESERVICEIMPL_H

#include <grpcpp/grpcpp.h>
#include "storage.grpc.pb.h"
#include "StorageNode.h"

class StorageNodeServiceImpl final : public storage::StorageNodeService::Service {
public:
    explicit StorageNodeServiceImpl(StorageNode* node);

    grpc::Status StoreChunk(grpc::ServerContext* context,
                            const storage::StoreChunkRequest* request,
                            storage::StoreChunkResponse* reply) override;

    grpc::Status RetrieveChunk(grpc::ServerContext* context,
                               const storage::RetrieveChunkRequest* request,
                               storage::ChunkData* reply) override;

    grpc::Status DeleteChunk(grpc::ServerContext* context,
                             const storage::DeleteChunkRequest* request,
                             storage::DeleteChunkResponse* reply) override;

    grpc::Status ReplicateChunk(grpc::ServerContext* context,
                                const storage::ReplicateChunkRequest* request,
                                storage::ReplicateChunkResponse* reply) override;

    grpc::Status HealthCheck(grpc::ServerContext* context,
                             const storage::HealthCheckRequest* request,
                             storage::HealthCheckResponse* reply) override;

private:
    StorageNode* node_;
};

#endif // STORAGENODESERVICEIMPL_H
