import asyncio
import hashlib

import grpc
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import Response

import storage_pb2       # type: ignore[attr-defined]
import storage_pb2_grpc  # type: ignore[attr-defined]
from etcd_discovery import EtcdDiscovery
from async_metadata_client import AsyncMetadataClient

app = FastAPI(title="RB Web Services — Storage API")
discovery = EtcdDiscovery()
metadata_client = AsyncMetadataClient(discovery)


def _store_chunk_sync(node_addr: str, chunk_id: str, data: bytes) -> str:
    """Send a chunk to a StorageNode via gRPC (sync — runs in executor)."""
    channel = grpc.insecure_channel(node_addr)
    stub = storage_pb2_grpc.StorageNodeServiceStub(channel)
    req = storage_pb2.StoreChunkRequest(chunk_id=chunk_id, data=data)  # type: ignore[attr-defined]
    resp = stub.StoreChunk(req, timeout=30)
    if not resp.success:
        raise RuntimeError(f"StorageNode at {node_addr} rejected chunk {chunk_id}")
    return resp.etag


def _retrieve_chunk_sync(node_addr: str, chunk_id: str) -> bytes:
    """Retrieve a chunk from a StorageNode via gRPC (sync — runs in executor)."""
    channel = grpc.insecure_channel(node_addr)
    stub = storage_pb2_grpc.StorageNodeServiceStub(channel)
    req = storage_pb2.RetrieveChunkRequest(chunk_id=chunk_id)  # type: ignore[attr-defined]
    resp = stub.RetrieveChunk(req, timeout=30)
    return resp.data


@app.put("/storage/{bucket}/{key:path}", summary="Upload an object")
async def upload_object(bucket: str, key: str, file: UploadFile = File(...)):
    """
    Upload a file into the distributed storage layer:
    1. Compute MD5 ETag from raw bytes.
    2. Register metadata with the Java MetadataServer (returns chunk locations).
    3. Push each chunk to its assigned StorageNode asynchronously.
    """
    content = await file.read()
    etag = hashlib.md5(content).hexdigest()

    try:
        meta = await metadata_client.create_file(
            bucket=bucket,
            key=key,
            owner="anonymous",
            size=len(content),
            content_type=file.content_type or "application/octet-stream",
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"MetadataServer error: {exc}")

    if not meta.chunks:
        raise HTTPException(status_code=500, detail="MetadataServer returned no chunk locations")

    loop = asyncio.get_event_loop()

    # For a single-chunk upload (current default) there is one entry.
    # For multi-chunk support, split `content` by chunk.size and iterate.
    try:
        await loop.run_in_executor(
            None,
            _store_chunk_sync,
            meta.chunks[0].storage_node,
            meta.chunks[0].chunk_id,
            content,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"StorageNode error: {exc}")

    return {"bucket": bucket, "key": key, "ETag": etag, "size": len(content)}


@app.get("/storage/{bucket}/{key:path}", summary="Download an object")
async def download_object(bucket: str, key: str):
    """
    Download a file from the distributed storage layer:
    1. Look up chunk locations in the Java MetadataServer.
    2. Retrieve each chunk from its StorageNode.
    3. Concatenate and return the raw bytes.
    """
    try:
        meta = await metadata_client.get_file(bucket=bucket, key=key)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"MetadataServer error: {exc}")

    if not meta.chunks:
        raise HTTPException(status_code=404, detail=f"{bucket}/{key} not found")

    loop = asyncio.get_event_loop()

    parts: list[bytes] = []
    for chunk in meta.chunks:
        try:
            data = await loop.run_in_executor(
                None,
                _retrieve_chunk_sync,
                chunk.storage_node,
                chunk.chunk_id,
            )
            parts.append(data)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"StorageNode error: {exc}")

    return Response(
        content=b"".join(parts),
        media_type=meta.content_type or "application/octet-stream",
        headers={"ETag": meta.etag},
    )


@app.delete("/storage/{bucket}/{key:path}", summary="Delete an object")
async def delete_object(bucket: str, key: str):
    try:
        result = await metadata_client.delete_file(bucket=bucket, key=key)
        return {"deleted": result.success}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"MetadataServer error: {exc}")


@app.get("/storage/{bucket}", summary="List objects in a bucket")
async def list_objects(bucket: str):
    try:
        files = await metadata_client.list_files(bucket=bucket)
        return {"bucket": bucket, "objects": [f.key for f in files]}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"MetadataServer error: {exc}")
