#include "StorageNode.h"
#include "StorageNodeServiceImpl.h"
#include "EtcdRegistration.h"
#include <fstream>
#include <iostream>
#include <chrono>
#include <thread>
#include <zlib.h>
#include <grpcpp/grpcpp.h>
#include <filesystem>

namespace fs = std::filesystem;

StorageNode::StorageNode(const std::string& id, const fs::path& path,
                         const std::string& etcd_endpoint, const std::string& grpc_addr)
    : node_id(id), storage_path(path) {
    if (!fs::exists(path)) {
        fs::create_directories(path);
    }
    total_storage = fs::space(path).capacity;
    used_storage  = 0;

    // Start etcd registration
    etcd_reg = std::make_unique<EtcdRegistration>(etcd_endpoint, node_id, grpc_addr);
    etcd_reg->start();

    // Start gRPC server
    grpc_service = std::make_unique<StorageNodeServiceImpl>(this);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(grpc_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(grpc_service.get());
    // Lift the default 4 MB message cap so large file chunks go through
    builder.SetMaxSendMessageSize(-1);
    builder.SetMaxReceiveMessageSize(-1);
    grpc_server = builder.BuildAndStart();
    std::cout << "[grpc] StorageNode gRPC server listening on " << grpc_addr << std::endl;

    // Start monitoring thread
    start_monitoring();
}

StorageNode::~StorageNode() {
    healthy = false;
    if (monitor_thread.joinable()) monitor_thread.join();
    if (grpc_server) grpc_server->Shutdown();
    if (etcd_reg) etcd_reg->stop();
}

bool StorageNode::write_chunk(const std::string& chunk_id, const std::vector<char>& data) {
    fs::path chunk_path = storage_path / chunk_id;
    std::ofstream ofs(chunk_path, std::ios::binary);
    if (!ofs) {
        std::cerr << "[node] Failed to open " << chunk_path << " for writing" << std::endl;
        return false;
    }
    ofs.write(data.data(), data.size());
    ofs.close();

    // Write CRC32 sidecar so read_chunk can detect bit rot
    uint32_t crc = calculate_crc32(data.data(), data.size());
    fs::path crc_path = storage_path / (chunk_id + ".crc");
    std::ofstream crc_ofs(crc_path);
    crc_ofs << crc;

    used_storage += data.size();
    std::cout << "[node] Stored chunk " << chunk_id
              << " (" << data.size() << " bytes, crc=" << crc << ")" << std::endl;
    return true;
}

std::vector<char> StorageNode::read_chunk(const std::string& chunk_id) {
    fs::path chunk_path = storage_path / chunk_id;
    std::ifstream ifs(chunk_path, std::ios::binary);
    if (!ifs) {
        std::cerr << "[node] Chunk not found: " << chunk_id << std::endl;
        return {};
    }
    std::vector<char> data(std::istreambuf_iterator<char>(ifs), {});

    // Verify CRC — returns empty (signals corruption) on mismatch
    fs::path crc_path = storage_path / (chunk_id + ".crc");
    std::ifstream crc_ifs(crc_path);
    if (crc_ifs) {
        uint32_t stored_crc = 0;
        crc_ifs >> stored_crc;
        uint32_t actual_crc = calculate_crc32(data.data(), data.size());
        if (stored_crc != actual_crc) {
            std::cerr << "[node] CRC mismatch for chunk " << chunk_id
                      << " (stored=" << stored_crc << " actual=" << actual_crc
                      << ") — treating as corrupt" << std::endl;
            return {};
        }
    }
    return data;
}

bool StorageNode::delete_chunk(const std::string& chunk_id) {
    fs::path chunk_path = storage_path / chunk_id;
    fs::path crc_path   = storage_path / (chunk_id + ".crc");
    bool removed = false;
    if (fs::exists(chunk_path)) {
        auto sz = fs::file_size(chunk_path);
        fs::remove(chunk_path);
        if (used_storage >= sz) used_storage -= sz;
        removed = true;
    }
    if (fs::exists(crc_path)) fs::remove(crc_path);
    if (removed)
        std::cout << "[node] Deleted chunk " << chunk_id << std::endl;
    else
        std::cerr << "[node] Chunk not found for delete: " << chunk_id << std::endl;
    return removed;
}

uint32_t StorageNode::calculate_crc32(const char* data, size_t len) {
    return crc32(0L, reinterpret_cast<const Bytef*>(data), len);
}

void StorageNode::start_monitoring() {
    monitor_thread = std::thread([this]() {
        while (healthy) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            check_disk_health();
        }
    });

    std::thread scrubber([this]() {
        while (healthy) {
            std::this_thread::sleep_for(std::chrono::hours(24 * 7));
            scrub_chunks();
        }
    });
    scrubber.detach();
}

void StorageNode::check_disk_health() {
    try {
        auto space = fs::space(storage_path);
        total_storage = space.capacity;
        if (space.available < 100 * 1024 * 1024) { // < 100 MB free
            std::cerr << "[node] WARNING: low disk space" << std::endl;
            healthy = false;
        }
    } catch (...) {}
}

bool StorageNode::ping(const std::string& peer) {
    // Simple gRPC health check to peer
    auto channel = grpc::CreateChannel(peer, grpc::InsecureChannelCredentials());
    auto stub = storage::StorageNodeService::NewStub(channel);
    storage::HealthCheckRequest req;
    storage::HealthCheckResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    auto status = stub->HealthCheck(&ctx, req, &resp);
    return status.ok() && resp.healthy();
}

void StorageNode::replicate_to_peers(const std::string& chunk_id, const std::vector<char>& data) {
    std::lock_guard<std::mutex> lock(peer_mutex);
    for (const auto& peer : peer_nodes) {
        if (peer == node_id) continue;
        auto channel = grpc::CreateChannel(peer, grpc::InsecureChannelCredentials());
        auto stub = storage::StorageNodeService::NewStub(channel);
        storage::StoreChunkRequest req;
        req.set_chunk_id(chunk_id);
        req.set_data(data.data(), data.size());
        storage::StoreChunkResponse resp;
        grpc::ClientContext ctx;
        stub->StoreChunk(&ctx, req, &resp);
    }
}

std::vector<char> StorageNode::read_from_peers(const std::string& chunk_id) {
    std::lock_guard<std::mutex> lock(peer_mutex);
    for (const auto& peer : peer_nodes) {
        if (peer == node_id) continue;
        auto channel = grpc::CreateChannel(peer, grpc::InsecureChannelCredentials());
        auto stub = storage::StorageNodeService::NewStub(channel);
        storage::RetrieveChunkRequest req;
        req.set_chunk_id(chunk_id);
        storage::ChunkData resp;
        grpc::ClientContext ctx;
        auto status = stub->RetrieveChunk(&ctx, req, &resp);
        if (status.ok()) {
            return std::vector<char>(resp.data().begin(), resp.data().end());
        }
    }
    return {};
}

void StorageNode::recover_chunks_from_peer(const std::string& /* failed_peer */) {
    // Placeholder: in production, list chunks stored at failed_peer and re-replicate
}

void StorageNode::scrub_chunks() {
    for (const auto& entry : fs::directory_iterator(storage_path)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() == ".crc") continue; // skip sidecar files
        std::string chunk_id = entry.path().filename().string();
        auto data = read_chunk(chunk_id);
        if (data.empty()) {
            std::cerr << "[scrub] Corrupt or missing chunk: " << chunk_id
                      << " — attempting peer recovery" << std::endl;
            std::lock_guard<std::mutex> lock(peer_mutex);
            for (const auto& peer : peer_nodes) {
                if (peer == node_id) continue;
                auto recovered = read_from_peers(chunk_id);
                if (!recovered.empty()) {
                    write_chunk(chunk_id, recovered);
                    std::cout << "[scrub] Recovered chunk " << chunk_id
                              << " from peer " << peer << std::endl;
                    break;
                }
            }
        }
    }
}
