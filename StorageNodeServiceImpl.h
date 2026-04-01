#ifndef STORAGENODE_H
#define STORAGENODE_H

#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <filesystem>
#include <memory>

#include <grpc++/grpc++.h>

// Forward declarations
class EtcdRegistration;
class StorageNodeServiceImpl;

namespace fs = std::filesystem;

class StorageNode {
public:
    StorageNode(const std::string& id, const fs::path& path,
                const std::string& etcd_endpoint, const std::string& grpc_addr);
    ~StorageNode();

    // Public API
    bool write_chunk(const std::string& chunk_id, const std::vector<char>& data);
    std::vector<char> read_chunk(const std::string& chunk_id);
    bool delete_chunk(const std::string& chunk_id);
    bool is_healthy() const { return healthy; }
    size_t get_used_space() const { return used_storage; }
    size_t get_total_space() const { return total_storage; }

private:
    std::string node_id;
    fs::path storage_path;
    std::atomic<size_t> total_storage;
    std::atomic<size_t> used_storage;
    std::vector<std::string> peer_nodes;
    std::atomic<bool> healthy{true};
    std::thread monitor_thread;
    std::mutex peer_mutex;

    // New integration members
    std::unique_ptr<EtcdRegistration> etcd_reg;
    std::unique_ptr<grpc::Server> grpc_server;
    std::unique_ptr<StorageNodeServiceImpl> grpc_service;

    // Internal helpers
    uint32_t calculate_crc32(const char* data, size_t len);
    void replicate_to_peers(const std::string& chunk_id, const std::vector<char>& data);
    std::vector<char> read_from_peers(const std::string& chunk_id);
    void start_monitoring();
    void check_disk_health();
    bool ping(const std::string& peer);
    void recover_chunks_from_peer(const std::string& failed_peer);
    void scrub_chunks();

public:
    // Block calling thread until the gRPC server shuts down
    void wait() { if (grpc_server) grpc_server->Wait(); }
};

#endif // STORAGENODE_H
