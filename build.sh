#include "StorageNodeServiceImpl.h"
#include <iostream>

StorageNodeServiceImpl::StorageNodeServiceImpl(StorageNode* node) : node_(node) {}

grpc::Status StorageNodeServiceImpl::StoreChunk(grpc::ServerContext* context,
                                                const storage::StoreChunkRequest* request,
                                                storage::StoreChunkResponse* reply) {
    std::string chunk_id = request->chunk_id();
    std::string data = request->data();
    bool ok = node_->write_chunk(chunk_id,
                std::vector<char>(data.begin(), data.end()));
    reply->set_success(ok);
    if (ok) {
        // Compute simple etag (first 8 bytes of data as hex, just for demo)
        // In practice, use MD5 or similar.
        std::string etag = std::to_string(std::hash<std::string>{}(data));
        reply->set_etag(etag);
    }
    return grpc::Status::OK;
}

grpc::Status StorageNodeServiceImpl::RetrieveChunk(grpc::ServerContext* context,
                                                   const storage::RetrieveChunkRequest* request,
                                                   storage::ChunkData* reply) {
    auto data = node_->read_chunk(request->chunk_id());
    if (data.empty()) {
        return grpc::Status(grpc::NOT_FOUND, "Chunk not found");
    }
    reply->set_data(data.data(), data.size());
    return grpc::Status::OK;
}

grpc::Status StorageNodeServiceImpl::DeleteChunk(grpc::ServerContext* context,
                                                 const storage::DeleteChunkRequest* request,
                                                 storage::DeleteChunkResponse* reply) {
    bool ok = node_->delete_chunk(request->chunk_id());
    reply->set_success(ok);
    return grpc::Status::OK;
}

grpc::Status StorageNodeServiceImpl::ReplicateChunk(grpc::ServerContext* context,
                                                    const storage::ReplicateChunkRequest* request,
                                                    storage::ReplicateChunkResponse* reply) {
    // This RPC tells this node to pull a chunk from the source node.
    std::string chunk_id = request->chunk_id();
    std::string source_node = request->target_node(); // actually source node
    // Open a client to source_node, retrieve chunk, then store locally.
    auto channel = grpc::CreateChannel(source_node, grpc::InsecureChannelCredentials());
    auto stub = storage::StorageNodeService::NewStub(channel);
    storage::RetrieveChunkRequest req;
    req.set_chunk_id(chunk_id);
    storage::ChunkData chunk_data;
    grpc::ClientContext client_ctx;
    auto status = stub->RetrieveChunk(&client_ctx, req, &chunk_data);
    if (!status.ok()) {
        reply->set_success(false);
        return grpc::Status::OK;
    }
    // Store locally
    std::vector<char> data(chunk_data.data().begin(), chunk_data.data().end());
    bool ok = node_->write_chunk(chunk_id, data);
    reply->set_success(ok);
    return grpc::Status::OK;
}

grpc::Status StorageNodeServiceImpl::HealthCheck(grpc::ServerContext* context,
                                                 const storage::HealthCheckRequest* request,
                                                 storage::HealthCheckResponse* reply) {
    reply->set_healthy(node_->is_healthy());
    reply->set_used_space(node_->get_used_space());
    reply->set_total_space(node_->get_total_space());
    return grpc::Status::OK;
}
