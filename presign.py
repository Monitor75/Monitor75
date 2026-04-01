import grpc
import storage_pb2, storage_pb2_grpc

class MetadataClient:
    def __init__(self, server_addr_or_channel):
        """Accept either an address string or a pre-created pooled channel."""
        if isinstance(server_addr_or_channel, str):
            self.channel = grpc.insecure_channel(server_addr_or_channel)
            self._owns_channel = True
        else:
            self.channel = server_addr_or_channel
            self._owns_channel = False
        self.stub = storage_pb2_grpc.MetadataServiceStub(self.channel)

    def create_file(self, bucket, key, owner, size, content_type):
        request = storage_pb2.CreateFileRequest(
            bucket=bucket,
            key=key,
            owner=owner,
            size=size,
            content_type=content_type
        )
        return self.stub.CreateFile(request)

    def get_file(self, bucket, key):
        request = storage_pb2.GetFileRequest(bucket=bucket, key=key)
        return self.stub.GetFile(request)

    def delete_file(self, bucket, key):
        request = storage_pb2.DeleteFileRequest(bucket=bucket, key=key)
        return self.stub.DeleteFile(request)

    def list_files(self, bucket):
        request = storage_pb2.ListFilesRequest(bucket=bucket)
        return self.stub.ListFiles(request)

    def get_chunk_locations(self, bucket, key):
        request = storage_pb2.GetChunkLocationsRequest(bucket=bucket, key=key)
        return self.stub.GetChunkLocations(request)
