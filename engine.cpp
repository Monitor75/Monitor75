#include <iostream>
#include <fstream>
#include <string>
#include <iomanip>
#include <sstream>
#include <vector>
#include <cstring>
#include <cstdint>
#include <sys/stat.h>
#include <unistd.h>

// XOR key — must match python/background_worker.py and main.py (ord('k') = 0x6B)
static const unsigned char XOR_KEY = 'k';

// Input is read in 256 KB chunks so XOR+write stays low-memory regardless
// of file size.  The raw bytes are also accumulated for SHA-256.
static const size_t BUF_SIZE = 256 * 1024;

// ── Assembly SHA-256 ──────────────────────────────────────────────────────────
// Implemented in sha256.asm (x86-64 System V ABI).
// Input must be pre-padded to a multiple of 64 bytes per FIPS 180-4.
extern "C" void sha256_asm(const unsigned char* input,
                            size_t              padded_length,
                            unsigned char*      output);

// Apply FIPS 180-4 SHA-256 message padding to `msg` and return the result.
// Appends: 0x80, zero bytes to reach len≡56 (mod 64), then 8-byte big-endian
// bit-length.  Output size is always a multiple of 64 bytes.
static std::vector<unsigned char> sha256_pad(const std::vector<unsigned char>& msg) {
    uint64_t bit_len = static_cast<uint64_t>(msg.size()) * 8;
    std::vector<unsigned char> padded(msg);

    padded.push_back(0x80);
    while (padded.size() % 64 != 56)
        padded.push_back(0x00);

    // Big-endian 64-bit message length
    for (int i = 7; i >= 0; --i)
        padded.push_back(static_cast<unsigned char>((bit_len >> (i * 8)) & 0xFF));

    return padded;
}

static std::string hex(const unsigned char* buf, size_t len) {
    std::ostringstream ss;
    for (size_t i = 0; i < len; ++i)
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)buf[i];
    return ss.str();
}

int main() {
    std::string tmp_path = "storage/.engine_tmp";
    mkdir("storage", 0777);

    std::ofstream tmp(tmp_path, std::ios::binary | std::ios::trunc);
    if (!tmp) { std::cerr << "cannot open tmp\n"; return 1; }

    // Accumulate original bytes for SHA-256 hashing while streaming
    // XOR-encrypted bytes to the temp file.
    std::vector<unsigned char> original;
    std::vector<unsigned char> buf(BUF_SIZE);
    bool got_data = false;

    while (true) {
        std::cin.read(reinterpret_cast<char*>(buf.data()), BUF_SIZE);
        std::streamsize n = std::cin.gcount();
        if (n <= 0) break;
        got_data = true;

        // Accumulate original (pre-encryption) bytes for hashing
        original.insert(original.end(), buf.data(), buf.data() + n);

        // XOR-encrypt in place and write to temp file
        for (std::streamsize i = 0; i < n; ++i)
            buf[i] ^= XOR_KEY;

        tmp.write(reinterpret_cast<char*>(buf.data()), n);
    }
    tmp.close();

    if (!got_data) {
        unlink(tmp_path.c_str());
        return 1;
    }

    // ── Compute SHA-256 via assembly ──────────────────────────────────────────
    std::vector<unsigned char> padded = sha256_pad(original);
    unsigned char hash[32];
    sha256_asm(padded.data(), padded.size(), hash);

    std::string file_hash = hex(hash, 32);

    // Content-addressed storage path: storage/<h0:2>/<h2:4>/<full_hash>
    std::string dir1      = "storage/" + file_hash.substr(0, 2);
    std::string dir2      = dir1 + "/" + file_hash.substr(2, 2);
    std::string file_path = dir2 + "/" + file_hash;

    mkdir(dir1.c_str(), 0777);
    mkdir(dir2.c_str(), 0777);

    // Atomic rename from temp → final content-addressed location
    if (rename(tmp_path.c_str(), file_path.c_str()) != 0) {
        std::cerr << "rename failed\n";
        unlink(tmp_path.c_str());
        return 1;
    }

    std::cout << file_hash;
    return 0;
}
