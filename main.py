from fastapi import (
    FastAPI,
    UploadFile,
    File,
    HTTPException,
    BackgroundTasks,
    Request,
    Query,
)
from fastapi.responses import FileResponse, Response, HTMLResponse
import asyncio
import json
import mimetypes
import os
import sys
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
import numpy as np

# Split files larger than this into multiple gRPC messages (10 MB per part).
# Prevents memory spikes and works around any per-message limits downstream.
GRPC_CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB

from database import (
    init_db,
    add_file_record,
    get_all_files,
    get_file_record,
    delete_file_record,
)
from native_bridge import inspect_file as native_inspect, NATIVE_AVAILABLE

# Make python/ modules importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "python"))
from metrics import chunk_writes, chunk_reads, active_connections, start_metrics_server
from gossip import GossipProtocol
from metadata_client import MetadataClient
from channel_pool import GrpcChannelPool
import presign as presign_mod
import background_worker as worker
import grpc
import storage_pb2
import storage_pb2_grpc

METADATA_SERVER_ADDR = os.environ.get("METADATA_SERVER_ADDR", "localhost:50051")
STORAGE_NODE_ADDR = os.environ.get("STORAGE_NODE_ADDR", "localhost:50052")

# All known MetadataServer and StorageNode addresses (for connection pools)
_META_ADDRS = ["localhost:50051", "localhost:50055", "localhost:50057"]
_STORAGE_ADDRS = ["localhost:50052", "localhost:50053", "localhost:50054"]

# How many StorageNodes each file is written to.
# 1 = no replication (current behaviour).
# 2 = primary + 1 replica (survives 1 node failure).
# 3 = primary + 2 replicas (survives 2 node failures).
REPLICATION_FACTOR = 2

# Populated at startup — reuse persistent gRPC channels across every request
_meta_pool: Optional[GrpcChannelPool] = None
_storage_pool: Optional[GrpcChannelPool] = None

ENGINE_PATH = "./engine"
METRICS_PORT = 8001  # Prometheus scrape endpoint (separate from the API on 5000)
GOSSIP_PORT = 5100  # UDP port this node gossips on

# Per-bucket website hosting config (index doc, error doc, enabled flag)
WEBSITE_CONFIG_DIR = os.path.join("storage", "websites")
os.makedirs(WEBSITE_CONFIG_DIR, exist_ok=True)


def _website_config(bucket: str) -> dict:
    """Load website hosting config for a bucket, or return defaults."""
    path = os.path.join(WEBSITE_CONFIG_DIR, f"{bucket}.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {
        "enabled": False,
        "index_document": "index.html",
        "error_document": "error.html",
    }


app = FastAPI(
    title="RB Web Services",
    version="2.0.0",
    description="A learning project: FastAPI + SQLite + C + x86-64 Assembly",
)

# Hard cap on upload size (500 MB). Prevents a single huge upload from filling
# /tmp and killing etcd / the JVM MetadataServers via disk-quota errors.
MAX_UPLOAD_BYTES = 500 * 1024 * 1024  # 500 MB


@app.middleware("http")
async def reject_oversized_requests(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_UPLOAD_BYTES:
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=413,
            content={
                "detail": f"File too large. Maximum upload size is {MAX_UPLOAD_BYTES // 1024 // 1024} MB."
            },
        )
    return await call_next(request)


STORAGE_DIR = "storage"
os.makedirs(STORAGE_DIR, exist_ok=True)

# Gossip instance — started in startup(), referenced here so it stays alive
_gossip: GossipProtocol | None = None


def hash_to_path(storage_hash: str) -> str:
    """
    Reconstruct the file path the C++ engine used when saving.
    Engine stores at: storage/<hash[0:2]>/<hash[2:4]>/<full-hash>
    """
    return os.path.join(STORAGE_DIR, storage_hash[:2], storage_hash[2:4], storage_hash)


@app.on_event("startup")
async def startup():
    """Initialize DB, metrics, gossip, and gRPC connection pools."""
    global _gossip, _meta_pool, _storage_pool
    await init_db()

    # Start Prometheus metrics HTTP server on a separate port (8001)
    threading.Thread(
        target=start_metrics_server, args=(METRICS_PORT,), daemon=True
    ).start()
    print(f"Metrics server started on port {METRICS_PORT}")

    # Start gossip node for cluster membership awareness
    try:
        _gossip = GossipProtocol("0.0.0.0", GOSSIP_PORT)
        print(f"Gossip node started on UDP port {GOSSIP_PORT}")
    except Exception as e:
        print(f"Gossip node could not start: {e}")

    # Pre-warm persistent gRPC channels to all backend nodes.
    # Channels are reused across every request instead of opened per-call.
    _meta_pool = GrpcChannelPool(_META_ADDRS)
    _storage_pool = GrpcChannelPool(_STORAGE_ADDRS)
    print(
        f"gRPC pool: {len(_META_ADDRS)} metadata channels, "
        f"{len(_STORAGE_ADDRS)} storage channels — ready"
    )


@app.get("/")
async def root():
    return {"message": "Welcome to RB WS! Visit /docs to explore the API."}


@app.post("/upload", summary="Upload a file")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a file to the cloud storage.
    - File data is piped through the C++ engine, which XOR-encrypts it with key 'K'
      and stores it at storage/<hash[0:2]>/<hash[2:4]>/<sha256-hash>.
    - The SHA256 hash returned by the engine is saved in the database to locate the file later.
    """
    if not os.path.exists(ENGINE_PATH):
        raise HTTPException(
            status_code=500,
            detail=f"Engine binary not found at '{ENGINE_PATH}'. Please compile engine.cpp first.",
        )

    raw_data = await file.read()

    try:
        result = subprocess.run(
            [ENGINE_PATH], input=raw_data, capture_output=True, timeout=30
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(
            status_code=500, detail="Engine timed out during encryption."
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Engine error: {str(e)}")

    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Engine exited with code {result.returncode}: {result.stderr.decode(errors='replace')}",
        )

    # The engine outputs the SHA256 hash — this is the file's unique storage key
    storage_hash = result.stdout.decode(errors="replace").strip()

    if not storage_hash or len(storage_hash) != 64:
        raise HTTPException(
            status_code=500,
            detail=f"Engine returned unexpected output: '{storage_hash}'",
        )

    # Confirm the engine actually wrote the file
    file_path = hash_to_path(storage_hash)
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=500,
            detail=f"Engine reported hash '{storage_hash}' but file not found on disk.",
        )

    file_size = os.path.getsize(file_path)

    await add_file_record(
        filename=file.filename,
        file_size=file_size,
        content_type=file.content_type or "unknown",
        storage_hash=storage_hash,
    )

    chunk_writes.inc()  # Prometheus: count every successful upload

    return {
        "message": "File uploaded and encrypted successfully!",
        "filename": file.filename,
        "size_bytes": file_size,
        "content_type": file.content_type,
        "storage_hash": storage_hash,
        "encrypted": True,
    }


@app.get("/files", summary="List all files (SQLite index)")
async def list_files():
    """
    Returns every file that has ever been uploaded through this server —
    both the legacy `POST /upload` endpoint and the distributed
    `PUT /storage/{bucket}/{key}` endpoint.

    Files uploaded via the distributed route are stored with
    `filename = "bucket/key"` and are returned with `bucket` and `key`
    as separate fields so they are easy to work with.

    For the authoritative, live list of files inside a specific bucket
    (sourced directly from the Raft MetadataServer cluster), use
    `GET /storage/{bucket}` instead.
    """
    raw = await get_all_files()
    enriched = []
    for record in raw:
        entry = dict(record)
        filename = entry.get("filename", "")
        # Distributed uploads store filename as "bucket/key".
        # Split them back so callers don't have to parse the string.
        if "/" in filename:
            parts = filename.split("/", 1)
            entry["bucket"] = parts[0]
            entry["key"] = parts[1]
        else:
            # Legacy single-file upload — no bucket
            entry["bucket"] = None
            entry["key"] = filename
        enriched.append(entry)
    return {"total_files": len(enriched), "files": enriched}


@app.get("/download/{filename}", summary="Download a file")
async def download_file(filename: str):
    """
    Download a file by its filename.
    Note: the file on disk is XOR-encrypted. This serves it as stored.
    """
    record = await get_file_record(filename)
    if not record:
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found.")

    storage_hash = record.get("storage_hash")
    if not storage_hash:
        raise HTTPException(
            status_code=500,
            detail=f"No storage hash for '{filename}'. File may have been uploaded before engine integration.",
        )

    file_path = hash_to_path(storage_hash)
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail=f"File '{filename}' found in DB but missing on disk at '{file_path}'.",
        )

    chunk_reads.inc()  # Prometheus: count every successful download
    active_connections.inc()  # Prometheus: track active transfers
    try:
        return FileResponse(
            path=file_path, filename=filename, media_type="application/octet-stream"
        )
    finally:
        active_connections.dec()


@app.get("/inspect/{filename}", summary="Inspect a file (C + Assembly powered)")
async def inspect_file(filename: str):
    """
    Deep-inspect a stored file using the native C + Assembly engine.

    Returns:
    - **adler32**: Adler-32 checksum (computed in x86-64 Assembly)
    - **file_size**: Total bytes
    - **null_bytes**: Count of 0x00 bytes
    - **newline_bytes**: Count of newline characters
    - **printable_bytes**: Count of visible ASCII characters
    - **min_byte / max_byte**: Byte value range
    - **is_text**: Whether the file appears to be plain text
    - **engine**: Whether native ASM or Python fallback was used
    """
    record = await get_file_record(filename)
    if not record:
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found.")

    storage_hash = record.get("storage_hash")
    if storage_hash:
        file_path = hash_to_path(storage_hash)
    else:
        # Legacy fallback for files uploaded before engine integration
        file_path = os.path.join(STORAGE_DIR, filename)

    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404, detail="File metadata found but file missing on disk."
        )

    try:
        stats = native_inspect(file_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Inspection failed: {str(e)}")

    return {
        "filename": filename,
        "storage_hash": storage_hash,
        "inspection": stats,
        "native_engine_loaded": NATIVE_AVAILABLE,
    }


@app.get("/engine-status", summary="Check if native ASM engine is loaded")
async def engine_status():
    """
    Check whether the C + Assembly shared library is loaded and active.
    If not, the API falls back to a pure Python implementation.

    To enable the native engine:
      cd native && make
    """
    return {
        "native_engine_available": NATIVE_AVAILABLE,
        "message": (
            "C + Assembly engine is active. Checksums computed in x86-64 ASM."
            if NATIVE_AVAILABLE
            else "Native engine not loaded. Using Python fallback. Run: cd native && make"
        ),
    }


def _get_metadata_client() -> MetadataClient:
    """Return a MetadataClient backed by a pooled, round-robin channel."""
    ch = _meta_pool.next_channel() if _meta_pool else None
    return MetadataClient(ch if ch is not None else METADATA_SERVER_ADDR)


def _storage_stub_for(node_addr: str):
    """Return a StorageNodeService stub using a persistent pooled channel."""
    ch = (
        _storage_pool.channel_for(node_addr)
        if _storage_pool
        else grpc.insecure_channel(node_addr)
    )
    return storage_pb2_grpc.StorageNodeServiceStub(ch)


def _replica_nodes_for(primary_addr: str) -> list[str]:
    """
    Return the StorageNode addresses that should hold replicas of a chunk
    stored on `primary_addr`.

    The replicas are chosen by rotating the global node list so the primary
    is first, then picking the next (REPLICATION_FACTOR - 1) entries.
    This gives a deterministic, spread-out assignment with no coordination.

    Example with 3 nodes and REPLICATION_FACTOR=2:
      primary=node0  →  replicas=[node1]
      primary=node1  →  replicas=[node2]
      primary=node2  →  replicas=[node0]
    """
    if REPLICATION_FACTOR <= 1 or len(_STORAGE_ADDRS) < 2:
        return []
    try:
        start = _STORAGE_ADDRS.index(primary_addr)
    except ValueError:
        start = 0
    count = min(REPLICATION_FACTOR - 1, len(_STORAGE_ADDRS) - 1)
    return [_STORAGE_ADDRS[(start + i + 1) % len(_STORAGE_ADDRS)] for i in range(count)]


def _fetch_chunk_from_node(
    chunk_id: str, node_addr: str, is_multipart: bool = False
) -> bytes:
    """
    Retrieve a chunk (or its numbered parts) from a single StorageNode.
    Returns the raw encrypted bytes, or raises grpc.RpcError on failure.
    Tries the single-id path first; if NOT_FOUND, probes _p0, _p1, … parts.
    """
    stub = _storage_stub_for(node_addr)

    def _get(cid: str) -> bytes:
        return stub.RetrieveChunk(
            storage_pb2.RetrieveChunkRequest(chunk_id=cid),  # type: ignore[attr-defined]
            timeout=30,
        ).data

    # Try simple (single-part) fetch first
    try:
        return _get(chunk_id)
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.NOT_FOUND:
            raise
    # Fall back to numbered parts: _p0, _p1, …
    idx, parts = 0, []
    while True:
        pid = f"{chunk_id}_p{idx}"
        try:
            parts.append(_get(pid))
            idx += 1
        except grpc.RpcError as pe:
            if pe.code() == grpc.StatusCode.NOT_FOUND:
                break
            raise
    if not parts:
        raise grpc.RpcError(f"No parts found for chunk {chunk_id}")  # type: ignore[abstract]
    return b"".join(parts)


async def _fetch_and_decrypt(bucket: str, key: str) -> tuple[bytes, str, str]:
    """
    Core fetch+decrypt pipeline shared by the storage API and website hosting:
      1. Look up metadata (chunk_id, primary node, etag) via MetadataServer.
      2. Try to retrieve encrypted bytes from the primary StorageNode.
         If the primary is unreachable or returns an error, automatically
         fall over to replica nodes (derived from REPLICATION_FACTOR).
      3. XOR-decrypt with numpy (C speed).

    Returns (decrypted_bytes, etag, content_type).
    Raises HTTPException(404) when the object does not exist on any node.
    """
    try:
        meta_client = _get_metadata_client()
        meta = meta_client.get_file(bucket=bucket, key=key)
        if not meta.chunks:
            raise HTTPException(status_code=404, detail=f"{bucket}/{key} not found")
        chunk_id = meta.chunks[0].chunk_id
        primary_addr = meta.chunks[0].storage_node
        etag = meta.etag
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail=f"{bucket}/{key} not found")
        raise HTTPException(
            status_code=502, detail=f"MetadataServer error: {e.details()}"
        )

    loop = asyncio.get_event_loop()

    # Build the ordered list of nodes to try: primary first, then replicas.
    # Replicas hold the exact same chunk_id so no extra metadata is needed.
    candidates = [primary_addr] + _replica_nodes_for(primary_addr)

    enc_bytes: Optional[bytes] = None
    last_error: Optional[Exception] = None

    for node_addr in candidates:
        try:
            enc_bytes = await loop.run_in_executor(
                None, _fetch_chunk_from_node, chunk_id, node_addr
            )
            break  # success — stop trying further nodes
        except grpc.RpcError as e:
            last_error = e
            # NOT_FOUND means this node genuinely doesn't have it (maybe it
            # was added after the upload).  Any other error is a node fault —
            # either way we fall through to the next candidate.
            continue
        except Exception as e:
            last_error = e
            continue

    if enc_bytes is None:
        detail = (
            str(last_error) if last_error else f"{bucket}/{key} not found on any node"
        )
        raise HTTPException(status_code=502, detail=f"StorageNode error: {detail}")

    dec_bytes = (np.frombuffer(enc_bytes, dtype=np.uint8) ^ ord("k")).tobytes()

    # Guess content-type from the key (filename) — e.g. .html → text/html
    content_type, _ = mimetypes.guess_type(key)
    content_type = content_type or "application/octet-stream"

    return dec_bytes, etag, content_type


@app.get(
    "/storage/{bucket}/{key:path}/presign",
    summary="Generate a pre-signed upload or download URL",
)
async def presign(
    request: Request,
    bucket: str,
    key: str,
    method: str = Query("PUT", description="HTTP method to authorise: PUT or GET"),
    ttl: int = Query(3600, description="Token lifetime in seconds"),
):
    """
    Return a time-limited signed URL that lets a client PUT or GET
    `bucket/key` directly — no session token required.

    The token is HMAC-SHA256 signed and automatically expires after `ttl`
    seconds.  Clients that use a PUT presigned URL bypass the standard
    authentication middleware and upload straight to this server, which
    then routes the chunk to the correct StorageNode.
    """
    base = str(request.base_url).rstrip("/")
    return presign_mod.generate(base, bucket, key, method=method.upper(), ttl=ttl)


@app.put(
    "/storage/{bucket}/{key:path}",
    summary="Distributed put: XOR-encrypt → Raft metadata → StorageNode",
)
async def distributed_put(
    request: Request,
    bucket: str,
    key: str,
    file: UploadFile = File(...),
    token: Optional[str] = Query(None),
    expires: Optional[int] = Query(None),
    method: Optional[str] = Query(None),
):
    """
    Upload a file into the distributed layer.

    **Normal flow** — multipart POST with no query params.

    **Pre-signed flow** — include `token`, `expires`, and `method=PUT` query
    params (obtained from the `/presign` endpoint).  This lets clients upload
    with a time-limited URL without holding a session credential.

    After the chunk is stored the server enqueues async background tasks
    (type detection, content analysis, pattern scan) without blocking the
    response.
    """
    # Validate pre-signed token when present
    if token and expires:
        if not presign_mod.validate(bucket, key, "PUT", token, expires):
            raise HTTPException(
                status_code=403, detail="Invalid or expired presigned token"
            )

    raw = await file.read()

    # Run the C++ engine in a thread pool so it never blocks the async event loop.
    # The engine reads from stdin, XOR-encrypts, writes to disk, prints the SHA256.
    if not os.path.exists(ENGINE_PATH):
        raise HTTPException(status_code=500, detail="Engine binary missing")

    loop = asyncio.get_event_loop()

    def _run_engine(data: bytes):
        r = subprocess.run([ENGINE_PATH], input=data, capture_output=True, timeout=120)
        return r

    result = await loop.run_in_executor(None, _run_engine, raw)
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Engine error: {result.stderr.decode(errors='replace')}",
        )
    storage_hash = result.stdout.decode().strip()
    if len(storage_hash) != 64:
        raise HTTPException(
            status_code=500, detail=f"Unexpected engine output: {storage_hash}"
        )

    # Read encrypted bytes from disk (offloaded to thread pool — non-blocking)
    enc_path = hash_to_path(storage_hash)
    enc_bytes: bytes = await loop.run_in_executor(
        None, lambda: open(enc_path, "rb").read()
    )

    # Register metadata via pooled gRPC channel (round-robins across 3 nodes)
    try:
        meta_client = _get_metadata_client()
        meta = meta_client.create_file(
            bucket=bucket,
            key=key,
            owner="anonymous",
            size=len(enc_bytes),
            content_type=file.content_type or "application/octet-stream",
        )
        chunk_id = meta.chunks[0].chunk_id
        node_addr = meta.chunks[0].storage_node
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MetadataServer error: {e}")

    # ----------------------------------------------------------------
    # Push encrypted data to the PRIMARY StorageNode, then replicate
    # to all replica nodes in parallel.
    #
    # All writes (primary + replicas) happen concurrently inside a
    # single ThreadPoolExecutor so the total latency is bounded by
    # the slowest node, not the sum of all node latencies.
    # ----------------------------------------------------------------
    parts = [
        enc_bytes[i : i + GRPC_CHUNK_SIZE]
        for i in range(0, len(enc_bytes), GRPC_CHUNK_SIZE)
    ]
    replica_addrs = _replica_nodes_for(node_addr)
    all_nodes = [node_addr] + replica_addrs  # primary always first

    def _store_to_node(target_addr: str) -> str:
        """Push all parts of this chunk to one StorageNode. Returns last etag."""
        s = _storage_stub_for(target_addr)
        last_tag = ""
        for idx, part in enumerate(parts):
            part_id = chunk_id if len(parts) == 1 else f"{chunk_id}_p{idx}"
            req = storage_pb2.StoreChunkRequest(chunk_id=part_id, data=part)  # type: ignore[attr-defined]
            resp = s.StoreChunk(req, timeout=60)
            if not resp.success:
                raise RuntimeError(f"StorageNode {target_addr} rejected part {idx}")
            last_tag = resp.etag
        return last_tag

    try:
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=len(all_nodes)) as pool:
            node_futures = [
                loop.run_in_executor(pool, _store_to_node, addr) for addr in all_nodes
            ]
            # gather() raises on first failure; replicas failing is non-fatal,
            # so we use return_exceptions=True and check primary separately.
            results = await asyncio.gather(*node_futures, return_exceptions=True)

        # Primary result is index 0 — must succeed.
        if isinstance(results[0], Exception):
            raise results[0]
        last_etag = results[0]

        # Log (but do not fail) replica errors so uploads always succeed.
        for addr, res in zip(replica_addrs, results[1:]):
            if isinstance(res, Exception):
                print(f"[WARN] Replica {addr} write failed: {res}")
    except grpc.RpcError as e:
        raise HTTPException(
            status_code=502, detail=f"StorageNode gRPC error: {e.details()}"
        )
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Enqueue async background analysis (non-blocking — does not delay response)
    await worker.enqueue(bucket, key, enc_bytes)

    chunk_writes.inc()

    # ----------------------------------------------------------------
    # Record in local SQLite so GET /files (and GET /storage/{bucket})
    # reflect this upload immediately.
    #
    # filename  → "bucket/key"  (parsed back into separate fields on read)
    # file_size → raw (pre-encryption) byte count — what the user uploaded
    # content_type → from multipart header, or guessed from key extension
    # storage_hash → SHA256 returned by engine — locates the file on disk
    # ----------------------------------------------------------------
    record_content_type = file.content_type or _mime_for_key(key)
    await add_file_record(
        filename=f"{bucket}/{key}",
        file_size=len(raw),
        content_type=record_content_type,
        storage_hash=storage_hash,
    )

    replica_status = {
        addr: ("ok" if not isinstance(res, Exception) else f"failed: {res}")
        for addr, res in zip(replica_addrs, results[1:])
    }
    return {
        "bucket": bucket,
        "key": key,
        "storage_hash": storage_hash,
        "chunk_id": chunk_id,
        "parts": len(parts),
        "storage_node": node_addr,
        "replicas": replica_status,
        "etag": last_etag,
        "encrypted": True,
    }


@app.get(
    "/storage/{bucket}/{key:path}/analysis",
    summary="Async analysis results for a stored object",
)
async def get_analysis(bucket: str, key: str):
    """
    Return the background-worker results for a previously uploaded object:
    detected MIME type, byte entropy, line/word counts (for text files),
    and whether any suspicious patterns were found.

    Results appear within milliseconds of the upload completing.
    Returns 404 if the object has never been uploaded or has been deleted.
    """
    result = await worker.get_result(bucket, key)
    if result is None:
        raise HTTPException(
            status_code=404, detail=f"No analysis available for {bucket}/{key}"
        )
    return result


def _parse_range_header(range_header: str, total: int) -> Optional[tuple[int, int]]:
    """
    Parse an HTTP Range header (RFC 7233) and return a (start, end) byte
    range that is inclusive on both ends and clamped to [0, total-1].

    Supports only the most common single-range form:  ``bytes=X-Y``

    Returns None when the header is absent, malformed, or unsatisfiable
    so the caller can fall back to sending the full response.
    """
    if not range_header:
        return None
    range_header = range_header.strip()
    if not range_header.lower().startswith("bytes="):
        return None
    spec = range_header[6:]  # everything after "bytes="
    if "," in spec:
        return None  # multi-range not supported; fall back to full response
    parts = spec.split("-")
    if len(parts) != 2:
        return None
    try:
        raw_start, raw_end = parts
        if raw_start == "":
            # Suffix range: bytes=-N  →  last N bytes
            n = int(raw_end)
            start = max(0, total - n)
            end = total - 1
        elif raw_end == "":
            # Open range: bytes=N-  →  from N to end
            start = int(raw_start)
            end = total - 1
        else:
            start = int(raw_start)
            end = int(raw_end)
    except ValueError:
        return None
    # Clamp and validate
    start = max(0, start)
    end = min(end, total - 1)
    if start > end:
        return None  # unsatisfiable
    return start, end


@app.get(
    "/storage/{bucket}/{key:path}",
    summary="Distributed get: metadata → StorageNode → XOR-decrypt",
)
async def distributed_get(
    request: Request,
    bucket: str,
    key: str,
    token: Optional[str] = Query(None),
    expires: Optional[int] = Query(None),
):
    """
    Download a file from the distributed layer.

    **Conditional GET** — Send `If-None-Match: "<etag>"` and receive
    `304 Not Modified` when your cached copy is still current.  Perfect for
    CDN cache revalidation without re-downloading the body.

    **Range requests (HTTP 206)** — Send a `Range: bytes=X-Y` header to
    receive only the bytes you need.  The server decrypts the full object,
    slices the requested range, and returns it with a `206 Partial Content`
    status and a `Content-Range: bytes X-Y/total` header.  Use cases:

    - **Video streaming** — the browser seeks to minute 3 by requesting the
      matching byte range; no need to download from the beginning.
    - **Download resume** — a client that lost its connection can restart from
      the last received byte rather than re-downloading the whole file.
    - **Partial read** — read just the header of a large binary file without
      fetching all of it.

    Only the ``bytes=X-Y`` single-range form is supported.  Multi-range
    requests fall back to a full 200 response.

    **Replica failover** — if the primary StorageNode is unavailable the
    server automatically retries on replica nodes (REPLICATION_FACTOR=2 by
    default) so a single dead node is transparent to the caller.

    **CDN headers** — every response includes `ETag` and
    `Cache-Control: public, max-age=3600` so CloudFront / Fastly / Cloudflare
    can serve objects from an edge without hitting this origin again.

    **Pre-signed flow** — include `token` + `expires` query params to
    authorise the download without a session credential.
    """
    # Validate pre-signed token when present
    if token and expires:
        if not presign_mod.validate(bucket, key, "GET", token, expires):
            raise HTTPException(
                status_code=403, detail="Invalid or expired presigned token"
            )

    # Fetch ETag from metadata first (cheap) for conditional-GET check
    try:
        meta_client = _get_metadata_client()
        meta = meta_client.get_file(bucket=bucket, key=key)
        if not meta.chunks:
            raise HTTPException(
                status_code=404, detail=f"{bucket}/{key} not found in metadata"
            )
        etag = meta.etag
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail=f"{bucket}/{key} not found")
        raise HTTPException(
            status_code=502, detail=f"MetadataServer error: {e.details()}"
        )

    quoted_etag = f'"{etag}"'

    # CDN / browser cache: honour If-None-Match → 304 Not Modified
    client_etag = request.headers.get("if-none-match", "")
    if client_etag and (client_etag == quoted_etag or client_etag == etag):
        return Response(
            status_code=304,
            headers={
                "ETag": quoted_etag,
                "Cache-Control": "public, max-age=3600",
            },
        )

    # Decrypt (with automatic replica fallback built into _fetch_and_decrypt)
    dec_bytes, _etag, content_type = await _fetch_and_decrypt(bucket, key)
    total = len(dec_bytes)

    common_headers = {
        "ETag": quoted_etag,
        "Cache-Control": "public, max-age=3600",
        "Vary": "Accept-Encoding",
        "Accept-Ranges": "bytes",
    }

    # ----------------------------------------------------------------
    # Range request handling (RFC 7233)
    # ----------------------------------------------------------------
    range_header = request.headers.get("range", "")
    byte_range = _parse_range_header(range_header, total)

    if byte_range is not None:
        start, end = byte_range
        body = dec_bytes[start : end + 1]
        chunk_reads.inc()
        return Response(
            content=body,
            status_code=206,
            media_type=content_type,
            headers={
                **common_headers,
                "Content-Range": f"bytes {start}-{end}/{total}",
                "Content-Length": str(len(body)),
            },
        )

    # Full response (no Range header or unsatisfiable range)
    chunk_reads.inc()
    return Response(
        content=dec_bytes,
        status_code=200,
        media_type=content_type,
        headers={
            **common_headers,
            "Content-Length": str(total),
        },
    )


@app.delete(
    "/storage/{bucket}/{key:path}",
    summary="Distributed delete: remove metadata and chunk",
)
async def distributed_delete(bucket: str, key: str):
    """
    Delete a file from the distributed layer:
    1. Look up chunk location from MetadataServer.
    2. Delete the chunk from the StorageNode.
    3. Remove the metadata entry via Raft (deleteFile).
    4. Discard any cached background-worker analysis results.
    """
    try:
        meta_client = _get_metadata_client()
        meta = meta_client.get_file(bucket=bucket, key=key)
        if not meta.chunks:
            raise HTTPException(status_code=404, detail=f"{bucket}/{key} not found")
        chunk_id = meta.chunks[0].chunk_id
        node_addr = meta.chunks[0].storage_node
    except grpc.RpcError as e:
        raise HTTPException(
            status_code=502, detail=f"MetadataServer error: {e.details()}"
        )

    try:
        stub = _storage_stub_for(node_addr)
        req = storage_pb2.DeleteChunkRequest(chunk_id=chunk_id)  # type: ignore[attr-defined]
        stub.DeleteChunk(req, timeout=5)
    except grpc.RpcError as e:
        raise HTTPException(status_code=502, detail=f"StorageNode error: {e.details()}")

    try:
        meta_client.delete_file(bucket=bucket, key=key)
    except grpc.RpcError as e:
        raise HTTPException(
            status_code=502, detail=f"MetadataServer delete error: {e.details()}"
        )

    await worker.remove_result(bucket, key)

    # Keep the SQLite index in sync — remove the record that was written
    # by distributed_put so GET /files no longer shows this file.
    await delete_file_record(f"{bucket}/{key}")

    return {"bucket": bucket, "key": key, "deleted": True}


@app.get(
    "/storage/{bucket}", summary="List all files in a bucket (from Raft MetadataServer)"
)
async def list_bucket(bucket: str):
    """
    Return every file stored in a bucket, sourced directly from the
    Raft MetadataServer cluster — the authoritative source of truth.

    Unlike `GET /files` (which reads the local SQLite index), this route
    queries the distributed metadata layer, so it is always accurate even
    if a file was uploaded before the SQLite recording fix was applied.

    Each entry includes:
    - `key`           — the file path within the bucket
    - `size`          — original (unencrypted) size in bytes
    - `content_type`  — MIME type detected at upload time
    - `etag`          — unique version tag for cache validation
    - `last_modified` — ISO-8601 timestamp from the MetadataServer
    - `storage_node`  — which C++ StorageNode holds the primary chunk
    - `chunk_id`      — the internal chunk UUID
    """
    try:
        meta_client = _get_metadata_client()
        resp = meta_client.list_files(bucket=bucket)
    except grpc.RpcError as e:
        raise HTTPException(
            status_code=502, detail=f"MetadataServer error: {e.details()}"
        )

    files = []
    for meta in resp.files:
        entry = {
            "bucket": meta.bucket,
            "key": meta.key,
            "size": meta.size,
            "content_type": _mime_for_key(meta.key),
            "etag": meta.etag,
            "last_modified": meta.last_modified,
            "owner": meta.owner,
            "chunk_count": len(meta.chunks),
        }
        if meta.chunks:
            entry["storage_node"] = meta.chunks[0].storage_node
            entry["chunk_id"] = meta.chunks[0].chunk_id
        files.append(entry)

    return {
        "bucket": bucket,
        "total_files": len(files),
        "files": files,
    }


# ---------------------------------------------------------------------------
# Static Website Hosting  (similar to AWS S3 / Netlify / Vercel hosting)
# ---------------------------------------------------------------------------

# Extended MIME type table.
# Python's built-in mimetypes module misses many modern web types.
# These overrides ensure the browser always gets the correct Content-Type.
_MIME_OVERRIDES: dict[str, str] = {
    # Web fonts
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".ttf": "font/ttf",
    ".otf": "font/otf",
    ".eot": "application/vnd.ms-fontobject",
    # Modern image formats
    ".webp": "image/webp",
    ".avif": "image/avif",
    ".heic": "image/heic",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
    # Video / audio
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".ogg": "audio/ogg",
    ".mp3": "audio/mpeg",
    ".flac": "audio/flac",
    # Web essentials
    ".js": "application/javascript",
    ".mjs": "application/javascript",
    ".json": "application/json",
    ".jsonld": "application/ld+json",
    ".xml": "application/xml",
    ".map": "application/json",  # source maps
    ".wasm": "application/wasm",
    ".css": "text/css; charset=utf-8",
    ".html": "text/html; charset=utf-8",
    ".htm": "text/html; charset=utf-8",
    ".txt": "text/plain; charset=utf-8",
    ".md": "text/markdown; charset=utf-8",
    ".csv": "text/csv; charset=utf-8",
    # Data / documents
    ".pdf": "application/pdf",
    ".zip": "application/zip",
    ".gz": "application/gzip",
    ".tar": "application/x-tar",
}

# File extensions that benefit from gzip compression.
_COMPRESSIBLE_TYPES: set[str] = {
    ".html",
    ".htm",
    ".css",
    ".js",
    ".mjs",
    ".json",
    ".jsonld",
    ".xml",
    ".svg",
    ".txt",
    ".md",
    ".csv",
    ".map",
}


# Cache-Control policy per content family.
# HTML:  no-cache → browser always revalidates (good for index.html updates).
# Fonts/immutable assets: 1 year (add a hash to the filename to bust).
# Images: 24 hours.  Everything else: 1 hour.
def _cache_control_for(ext: str) -> str:
    if ext in {".html", ".htm"}:
        return "no-cache"
    if ext in {".woff", ".woff2", ".ttf", ".otf", ".eot"}:
        return "public, max-age=31536000, immutable"
    if ext in {".css", ".js", ".mjs", ".wasm"}:
        return "public, max-age=31536000, immutable"
    if ext in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif", ".svg", ".ico"}:
        return "public, max-age=86400"
    return "public, max-age=3600"


def _mime_for_key(key: str) -> str:
    """Return the best Content-Type for a storage key based on its extension."""
    ext = os.path.splitext(key)[1].lower()
    if ext in _MIME_OVERRIDES:
        return _MIME_OVERRIDES[ext]
    guessed, _ = mimetypes.guess_type(key)
    return guessed or "application/octet-stream"


def _maybe_gzip(body: bytes, key: str, accept_encoding: str) -> tuple[bytes, bool]:
    """
    Compress `body` with gzip if:
      - The client advertises Accept-Encoding: gzip
      - The file extension is in the compressible list
      - The compressed result is actually smaller (guards against tiny files)
    Returns (body, was_compressed).
    """
    import gzip as _gzip

    ext = os.path.splitext(key)[1].lower()
    if "gzip" not in accept_encoding or ext not in _COMPRESSIBLE_TYPES:
        return body, False
    compressed = _gzip.compress(body, compresslevel=6)
    if len(compressed) >= len(body):
        return body, False
    return compressed, True


@app.put(
    "/site/{bucket}",
    summary="Enable / configure static website hosting for a bucket",
)
async def configure_website(bucket: str, request: Request):
    """
    Enable static website hosting for a bucket.

    All fields are optional — omit any to keep the default:

    ```json
    {
      "index_document": "index.html",
      "error_document": "error.html",
      "spa_mode":       false,
      "cors_origin":    "*",
      "cors_methods":   "GET, HEAD, OPTIONS"
    }
    ```

    **`spa_mode`** — when `true`, any 404 serves `index_document` instead of
    `error_document`.  Use this for React / Vue / Angular apps where the
    JavaScript router handles all paths client-side.

    **`cors_origin`** — sets `Access-Control-Allow-Origin`.  Required when
    fonts or API responses are fetched from a different domain.  Use `"*"` for
    fully public assets, or a specific origin like `"https://myapp.com"`.

    Once enabled, the bucket is accessible at `GET /site/{bucket}/`.
    """
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    config = {
        "enabled": True,
        "index_document": body.get("index_document", "index.html"),
        "error_document": body.get("error_document", "error.html"),
        "spa_mode": bool(body.get("spa_mode", False)),
        "cors_origin": body.get("cors_origin", None),
        "cors_methods": body.get("cors_methods", "GET, HEAD, OPTIONS"),
    }
    path = os.path.join(WEBSITE_CONFIG_DIR, f"{bucket}.json")
    with open(path, "w") as f:
        json.dump(config, f, indent=2)

    return {
        "bucket": bucket,
        "website_enabled": True,
        "index_document": config["index_document"],
        "error_document": config["error_document"],
        "spa_mode": config["spa_mode"],
        "cors_origin": config["cors_origin"],
        "website_url": f"/site/{bucket}/",
    }


@app.get("/site/{bucket}", summary="Show website hosting config for a bucket")
async def get_website_config(bucket: str):
    """Return the current website hosting configuration for the bucket."""
    cfg = _website_config(bucket)
    return {
        "bucket": bucket,
        "website_enabled": cfg["enabled"],
        "index_document": cfg["index_document"],
        "error_document": cfg["error_document"],
        "spa_mode": cfg.get("spa_mode", False),
        "cors_origin": cfg.get("cors_origin", None),
        "cors_methods": cfg.get("cors_methods", "GET, HEAD, OPTIONS"),
        "website_url": f"/site/{bucket}/" if cfg["enabled"] else None,
    }


@app.delete("/site/{bucket}", summary="Disable website hosting for a bucket")
async def disable_website(bucket: str):
    """
    Disable static website hosting for a bucket.
    The files inside the bucket are not deleted — only the hosting config is removed.
    """
    path = os.path.join(WEBSITE_CONFIG_DIR, f"{bucket}.json")
    if os.path.exists(path):
        os.remove(path)
    return {"bucket": bucket, "website_enabled": False}


@app.get(
    "/site/{bucket}/{key:path}",
    summary="Serve a file from a static website bucket",
)
async def serve_website(bucket: str, key: str, request: Request):
    """
    Serve files from a bucket configured for static website hosting.

    Behaves like a real web host (Netlify / Vercel / S3 website endpoint):

    **Decryption** — every file is stored XOR-encrypted with key `'k'`.
    This route decrypts transparently before sending; the browser never
    sees encrypted bytes.

    **Content-Type** — determined from the file extension using an extended
    MIME table that covers `.woff2`, `.webp`, `.avif`, `.wasm`, `.mp4`, and
    30+ other modern types that Python's built-in detector misses.

    **Inline delivery** — `Content-Disposition: inline` so the browser
    always renders HTML/images/video rather than prompting a download.

    **Gzip compression** — HTML, CSS, JS, JSON, SVG, and other text assets
    are compressed on the fly when the browser sends `Accept-Encoding: gzip`.
    Typically 60-80% smaller over the wire.

    **Smart cache headers** — tailored per file type:
    - HTML → `no-cache` (always revalidate so deploys appear immediately)
    - CSS / JS / fonts → `max-age=31536000, immutable` (1 year; use hashed filenames)
    - Images → `max-age=86400` (24 hours)
    - Everything else → `max-age=3600` (1 hour)

    **Security headers** — `X-Content-Type-Options: nosniff` prevents MIME
    sniffing attacks.  `X-Frame-Options: SAMEORIGIN` blocks clickjacking.
    `Referrer-Policy` controls what the browser leaks on navigation.

    **CORS** — when `cors_origin` is set on the bucket config, the matching
    `Access-Control-Allow-Origin` header is sent.  Essential for fonts and
    fetch() calls from other domains.

    **Index document** — trailing-slash and empty-key requests automatically
    serve the configured `index_document` (default `index.html`).

    **SPA mode** — when `spa_mode: true`, any 404 serves the index document
    so the JavaScript router (React / Vue / Angular) handles the path.

    **Custom error page** — when `spa_mode: false`, missing files serve the
    bucket's `error_document` (default `error.html`) with HTTP 404.

    **Conditional GET** — `If-None-Match` → `304 Not Modified` so repeat
    visits skip the body entirely when the file has not changed.
    """
    cfg = _website_config(bucket)
    if not cfg["enabled"]:
        raise HTTPException(
            status_code=403,
            detail=(
                f"Website hosting is not enabled for bucket '{bucket}'. "
                f"Enable it with  PUT /site/{bucket}"
            ),
        )

    # Resolve directory-style requests to the index document
    if not key or key.endswith("/"):
        key = (key or "") + cfg["index_document"]

    accept_encoding = request.headers.get("accept-encoding", "")
    spa_mode = cfg.get("spa_mode", False)
    cors_origin = cfg.get("cors_origin", None)
    cors_methods = cfg.get("cors_methods", "GET, HEAD, OPTIONS")

    async def _serve(k: str, status: int = 200) -> Response:
        dec_bytes, etag, _ct = await _fetch_and_decrypt(bucket, k)
        quoted_etag = f'"{etag}"'
        total = len(dec_bytes)

        # Conditional GET — skip body when ETag matches
        client_etag = request.headers.get("if-none-match", "")
        if client_etag and (client_etag == quoted_etag or client_etag == etag):
            return Response(
                status_code=304,
                headers={
                    "ETag": quoted_etag,
                    "Cache-Control": _cache_control_for(os.path.splitext(k)[1].lower()),
                },
            )

        content_type = _mime_for_key(k)
        ext = os.path.splitext(k)[1].lower()
        cache_ctrl = _cache_control_for(ext)

        # ── Range request (HTTP 206 Partial Content) ────────────────────
        # Required for browsers to seek inside video/audio files, resume
        # large downloads, and stream WebAssembly or other binary assets.
        # We skip gzip compression on partial responses because the
        # Content-Encoding would apply to the whole file, not the slice.
        range_header = request.headers.get("range", "")
        if range_header and status == 200:
            byte_range = _parse_range_header(range_header, total)
            if byte_range is None:
                # Unsatisfiable range → 416
                return Response(
                    status_code=416,
                    headers={
                        "Content-Range": f"bytes */{total}",
                        "Accept-Ranges": "bytes",
                    },
                )
            start, end = byte_range
            chunk = dec_bytes[start : end + 1]
            return Response(
                content=chunk,
                status_code=206,
                media_type=content_type,
                headers={
                    "Content-Range":   f"bytes {start}-{end}/{total}",
                    "Content-Length":  str(len(chunk)),
                    "Accept-Ranges":   "bytes",
                    "ETag":            quoted_etag,
                    "Cache-Control":   cache_ctrl,
                    "Content-Disposition": "inline",
                    "X-Content-Type-Options": "nosniff",
                },
            )

        # ── Full response ────────────────────────────────────────────────
        # Gzip compression only for compressible text assets (not binary)
        body, gzipped = _maybe_gzip(dec_bytes, k, accept_encoding)

        headers: dict[str, str] = {
            # Identity & caching
            "ETag":            quoted_etag,
            "Cache-Control":   cache_ctrl,
            "Vary":            "Accept-Encoding",
            # Tell clients they can make range requests on any file
            "Accept-Ranges":   "bytes",
            # Always render inline — never prompt a download dialog
            "Content-Disposition": "inline",
            # Security headers
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options":        "SAMEORIGIN",
            "Referrer-Policy":        "strict-origin-when-cross-origin",
            "X-XSS-Protection":       "1; mode=block",
        }

        if gzipped:
            headers["Content-Encoding"] = "gzip"

        # CORS — send only when configured on the bucket
        if cors_origin:
            headers["Access-Control-Allow-Origin"]  = cors_origin
            headers["Access-Control-Allow-Methods"] = cors_methods
            headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"

        return Response(
            content=body,
            status_code=status,
            media_type=content_type,
            headers=headers,
        )

    # --- Try serving the requested key ---
    try:
        return await _serve(key)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise

    # --- 404 path ---
    if spa_mode:
        # SPA mode: serve index.html for every unknown path so the JS
        # router (React / Vue / Angular) can handle client-side routing.
        try:
            return await _serve(cfg["index_document"], status=200)
        except HTTPException:
            pass

    # Non-SPA: serve the configured error document with 404 status
    try:
        return await _serve(cfg["error_document"], status=404)
    except HTTPException:
        pass

    # Last resort — built-in HTML 404 when no error doc exists either
    return HTMLResponse(
        content=(
            "<!DOCTYPE html><html><head>"
            "<title>404 Not Found</title>"
            "<style>body{font-family:sans-serif;padding:2rem;color:#333}"
            "h1{color:#c0392b}code{background:#f4f4f4;padding:.2em .4em;"
            "border-radius:3px}</style></head><body>"
            f"<h1>404 Not Found</h1>"
            f"<p><code>{bucket}/{key}</code> does not exist in this bucket.</p>"
            f"<p><a href='/site/{bucket}/'>← Back to home</a></p>"
            "</body></html>"
        ),
        status_code=404,
    )


@app.get("/cluster-status", summary="Show gossip cluster membership")
async def cluster_status():
    """
    Returns the current view of the cluster as seen by this node's gossip protocol.
    Each entry shows the node address and its state: alive, suspect, or dead.
    """
    if _gossip is None:
        return {"gossip_active": False, "members": {}}
    with _gossip.lock:
        members = {
            k: {
                "host": v.host,
                "port": v.port,
                "state": v.state,
                "incarnation": v.incarnation,
            }
            for k, v in _gossip.members.items()
        }
    return {
        "gossip_active": True,
        "gossip_port": GOSSIP_PORT,
        "metrics_port": METRICS_PORT,
        "member_count": len(members),
        "members": members,
    }


@app.delete("/delete/{filename}", summary="Delete a file")
async def delete_file(filename: str):
    """
    Delete a file by its filename.
    Removes both the encrypted file from disk and its record from the database.
    """
    record = await get_file_record(filename)
    if not record:
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found.")

    storage_hash = record.get("storage_hash")
    if storage_hash:
        file_path = hash_to_path(storage_hash)
    else:
        file_path = os.path.join(STORAGE_DIR, filename)

    if os.path.exists(file_path):
        os.remove(file_path)

    await delete_file_record(filename)

    return {
        "message": f"File '{filename}' deleted successfully.",
        "storage_hash": storage_hash,
    }
