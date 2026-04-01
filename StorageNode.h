#pragma once
#include <string>
#include <vector>
#include <thread>
#include <atomic>

class EtcdRegistration {
public:
    EtcdRegistration(const std::string& etcd_endpoints,
                     const std::string& node_id,
                     const std::string& node_addr);
    ~EtcdRegistration();
    void start();
    void stop();

private:
    std::vector<std::string> etcd_endpoints_;
    std::string etcd_base_;      // currently active endpoint
    std::string node_id_;
    std::string node_addr_;
    std::atomic<bool> running_;
    std::string lease_id_;
    std::thread keepalive_thread_;

    // Try each endpoint in order; updates etcd_base_ on success
    std::string post_with_fallback(const std::string& path, const std::string& body);
};
