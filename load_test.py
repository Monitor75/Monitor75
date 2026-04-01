"""
load_test.py — Parallel load tester for the Mini Cloud Storage API
==================================================================

Simulates multiple concurrent clients hitting your API at the same time.
Measures: requests/sec, average latency, min/max latency, error rate.

Usage:
  python load_test.py

What it tests:
  - GET /          (lightweight, no DB)
  - GET /files     (DB read)
  - GET /engine-status (native lib check)
  - POST /upload   (DB write + disk I/O — the stress point)

Run your server first:
  uvicorn main:app --host 0.0.0.0 --port 5000
"""

import threading
import requests
import time
import os
import tempfile
import statistics

BASE_URL = "http://localhost:5000"


# -------------------------------------------------------
# Helper: create a small temp file to use for uploads
# -------------------------------------------------------
def make_temp_file(size_bytes=1024):
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
    f.write(b"A" * size_bytes)
    f.close()
    return f.name


# -------------------------------------------------------
# Worker: one simulated client making one request
# -------------------------------------------------------
def worker(method, url, results, files=None):
    start = time.perf_counter()
    try:
        if method == "GET":
            r = requests.get(url, timeout=10)
        elif method == "POST" and files:
            with open(files, "rb") as f:
                r = requests.post(url, files={"file": f}, timeout=10)
        else:
            results.append({"ok": False, "latency": 0, "status": 0})
            return

        elapsed = time.perf_counter() - start
        results.append({
            "ok": r.status_code < 400,
            "latency": elapsed,
            "status": r.status_code
        })
    except Exception as e:
        elapsed = time.perf_counter() - start
        results.append({"ok": False, "latency": elapsed, "status": -1, "error": str(e)})


# -------------------------------------------------------
# Run: fire N concurrent threads all at once
# -------------------------------------------------------
def run_concurrent(method, url, n, files=None):
    results = []
    threads = []

    for _ in range(n):
        t = threading.Thread(target=worker, args=(method, url, results, files))
        threads.append(t)

    wall_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall_end = time.perf_counter()

    wall_time = wall_end - wall_start
    latencies = [r["latency"] for r in results if r["ok"]]
    errors = [r for r in results if not r["ok"]]

    return {
        "total": n,
        "success": len(latencies),
        "errors": len(errors),
        "wall_time_sec": round(wall_time, 3),
        "req_per_sec": round(n / wall_time, 1) if wall_time > 0 else 0,
        "avg_latency_ms": round(statistics.mean(latencies) * 1000, 1) if latencies else 0,
        "min_latency_ms": round(min(latencies) * 1000, 1) if latencies else 0,
        "max_latency_ms": round(max(latencies) * 1000, 1) if latencies else 0,
        "p50_latency_ms": round(statistics.median(latencies) * 1000, 1) if latencies else 0,
    }


# -------------------------------------------------------
# Print a formatted result row
# -------------------------------------------------------
def print_result(label, concurrency, result):
    status = "✅" if result["errors"] == 0 else f"⚠️  {result['errors']} errors"
    print(f"  {label:30s} | clients={concurrency:3d} | "
          f"{result['req_per_sec']:6.1f} req/s | "
          f"avg={result['avg_latency_ms']:6.1f}ms | "
          f"min={result['min_latency_ms']:5.1f}ms | "
          f"max={result['max_latency_ms']:6.1f}ms | {status}")


# -------------------------------------------------------
# Main test suite
# -------------------------------------------------------
def main():
    print("\n" + "=" * 90)
    print("  Mini Cloud Storage API — Load Test")
    print("  Target:", BASE_URL)
    print("=" * 90)

    # Check server is up
    try:
        r = requests.get(BASE_URL, timeout=5)
        print(f"\n  Server is UP ✅  (status {r.status_code})\n")
    except Exception as e:
        print(f"\n  ❌ Cannot reach server at {BASE_URL}")
        print(f"     Error: {e}")
        print("     Make sure: uvicorn main:app --host 0.0.0.0 --port 5000\n")
        return

    concurrency_levels = [10, 50, 100]

    # ---- Test 1: GET / ----
    print("\n[ Test 1: GET / — lightweight root endpoint ]")
    print(f"  {'Endpoint':30s} | {'clients':>7s} | {'throughput':>10s} | {'avg':>9s} | {'min':>8s} | {'max':>9s} | status")
    print("  " + "-" * 84)
    for n in concurrency_levels:
        result = run_concurrent("GET", f"{BASE_URL}/", n)
        print_result("GET /", n, result)

    # ---- Test 2: GET /files ----
    print("\n[ Test 2: GET /files — SQLite read ]")
    print(f"  {'Endpoint':30s} | {'clients':>7s} | {'throughput':>10s} | {'avg':>9s} | {'min':>8s} | {'max':>9s} | status")
    print("  " + "-" * 84)
    for n in concurrency_levels:
        result = run_concurrent("GET", f"{BASE_URL}/files", n)
        print_result("GET /files", n, result)

    # ---- Test 3: GET /engine-status ----
    print("\n[ Test 3: GET /engine-status — native library check ]")
    print(f"  {'Endpoint':30s} | {'clients':>7s} | {'throughput':>10s} | {'avg':>9s} | {'min':>8s} | {'max':>9s} | status")
    print("  " + "-" * 84)
    for n in concurrency_levels:
        result = run_concurrent("GET", f"{BASE_URL}/engine-status", n)
        print_result("GET /engine-status", n, result)

    # ---- Test 4: POST /upload ----
    print("\n[ Test 4: POST /upload — SQLite write + disk I/O (the stress point) ]")
    print(f"  {'Endpoint':30s} | {'clients':>7s} | {'throughput':>10s} | {'avg':>9s} | {'min':>8s} | {'max':>9s} | status")
    print("  " + "-" * 84)
    tmp = make_temp_file(4096)
    for n in concurrency_levels:
        result = run_concurrent("POST", f"{BASE_URL}/upload", n, files=tmp)
        print_result("POST /upload", n, result)
    os.unlink(tmp)

    print("\n" + "=" * 90)
    print("  Load test complete.")
    print("  Key things to observe:")
    print("   - GET endpoints should handle 100 clients with low latency (async I/O)")
    print("   - POST /upload may show errors or high latency at 100 clients (SQLite write lock)")
    print("   - req/s drops as concurrency rises → your bottleneck is exposed")
    print("=" * 90 + "\n")


if __name__ == "__main__":
    main()
