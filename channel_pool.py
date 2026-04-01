import grpc
import grpc.aio
from typing import List
import storage_pb2        # type: ignore[attr-defined]
import storage_pb2_grpc  # type: ignore[attr-defined]


class AsyncMetadataClient:
    def __init__(self, etcd_discovery):
        self.discovery = etcd_discovery
        self.servers: List[str] = []
        self.current = 0

    async def _get_channel(self, addr: str):
        return grpc.aio.insecure_channel(addr)

    async def _call(self, method, request, retries=3):
        for attempt in range(retries):
            if not self.servers:
                self.servers = await self.discovery.get_metadata_servers()
                if not self.servers:
                    raise RuntimeError("No metadata servers found")
            addr = self.servers[self.current % len(self.servers)]
            self.current += 1
            try:
                async with await self._get_channel(addr) as channel:
                    stub = storage_pb2_grpc.MetadataServiceStub(channel)
                    return await method(stub, request)
            except grpc.aio.AioRpcError as e:
                print(f"Error calling {addr}: {e}")
                self.servers = []
                if attempt == retries - 1:
                    raise
        raise Exception("All metadata servers failed")

    async def create_file(self, bucket, key, owner, size, content_type):
        req = storage_pb2.CreateFileRequest(
            bucket=bucket, key=key, owner=owner, size=size, content_type=content_type
        )
        return await self._call(lambda stub, r: stub.CreateFile(r), req)

    async def get_file(self, bucket, key):
        req = storage_pb2.GetFileRequest(bucket=bucket, key=key)
        return await self._call(lambda stub, r: stub.GetFile(r), req)

    async def delete_file(self, bucket, key):
        req = storage_pb2.DeleteFileRequest(bucket=bucket, key=key)
        return await self._call(lambda stub, r: stub.DeleteFile(r), req)

    async def list_files(self, bucket):
        req = storage_pb2.ListFilesRequest(bucket=bucket)
        resp = await self._call(lambda stub, r: stub.ListFiles(r), req)
        return resp.files
