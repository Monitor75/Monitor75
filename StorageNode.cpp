// EtcdRegistration.cpp
// Registers this storage node in etcd using the v3 HTTP/JSON REST API.
// Supports a comma-separated list of etcd endpoints for HA.
#include "EtcdRegistration.h"
#include <curl/curl.h>
#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>
#include <atomic>
#include <cstring>

// ---- helpers ----------------------------------------------------------------

static std::string base64_encode(const std::string& in) {
    static const char* b64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    int val = 0, valb = -6;
    for (unsigned char c : in) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) { out.push_back(b64[(val >> valb) & 0x3F]); valb -= 6; }
    }
    if (valb > -6) out.push_back(b64[((val << 8) >> (valb + 8)) & 0x3F]);
    while (out.size() % 4) out.push_back('=');
    return out;
}

static size_t write_cb(char* ptr, size_t size, size_t nmemb, std::string* data) {
    data->append(ptr, size * nmemb);
    return size * nmemb;
}

static std::string http_post(const std::string& url, const std::string& body) {
    CURL* curl = curl_easy_init();
    std::string response;
    if (!curl) return "";
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
    curl_easy_perform(curl);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return response;
}

static std::string extract_json_str(const std::string& json, const std::string& key) {
    std::string search = "\"" + key + "\":\"";
    auto pos = json.find(search);
    if (pos == std::string::npos) return "";
    pos += search.size();
    auto end = json.find("\"", pos);
    return json.substr(pos, end - pos);
}

// ---- EtcdRegistration -------------------------------------------------------

EtcdRegistration::EtcdRegistration(const std::string& etcd_endpoints,
                                   const std::string& node_id,
                                   const std::string& node_addr)
    : node_id_(node_id), node_addr_(node_addr), running_(false), lease_id_("") {
    std::istringstream ss(etcd_endpoints);
    std::string ep;
    while (std::getline(ss, ep, ',')) {
        if (!ep.empty()) etcd_endpoints_.push_back(ep);
    }
    etcd_base_ = etcd_endpoints_.empty() ? "http://127.0.0.1:2379" : etcd_endpoints_[0];
}

// Try each endpoint in order; switches etcd_base_ to the first one that responds.
std::string EtcdRegistration::post_with_fallback(const std::string& path,
                                                  const std::string& body) {
    for (const auto& base : etcd_endpoints_) {
        std::string resp = http_post(base + path, body);
        if (!resp.empty()) {
            etcd_base_ = base;
            return resp;
        }
    }
    return "";
}

void EtcdRegistration::start() {
    running_ = true;

    // 1. Grant a 10-second lease (try all endpoints)
    std::string resp = post_with_fallback("/v3/lease/grant", R"({"TTL":10,"ID":0})");
    lease_id_ = extract_json_str(resp, "ID");
    if (lease_id_.empty()) {
        std::cerr << "[etcd] Failed to obtain lease, resp: " << resp << std::endl;
        return;
    }
    std::cout << "[etcd] Lease granted: " << lease_id_ << std::endl;

    // 2. Register key=/services/storage/<node_id> -> node_addr with lease
    std::string k = base64_encode("/services/storage/" + node_id_);
    std::string v = base64_encode(node_addr_);
    post_with_fallback("/v3/kv/put",
        R"({"key":")" + k + R"(","value":")" + v + R"(","lease":")" + lease_id_ + R"("})");
    std::cout << "[etcd] Registered /services/storage/" << node_id_
              << " -> " << node_addr_ << std::endl;

    // 3. Keep-alive thread: renew lease every 5 seconds, with endpoint fallback
    keepalive_thread_ = std::thread([this]() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            if (!running_) break;
            post_with_fallback("/v3/lease/keepalive",
                R"({"ID":")" + lease_id_ + R"("})");
        }
    });
}

void EtcdRegistration::stop() {
    running_ = false;
    if (keepalive_thread_.joinable()) keepalive_thread_.join();
    if (!lease_id_.empty()) {
        post_with_fallback("/v3/lease/revoke",
            R"({"ID":")" + lease_id_ + R"("})");
        std::cout << "[etcd] Lease revoked" << std::endl;
    }
}

EtcdRegistration::~EtcdRegistration() { stop(); }
