import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import storage.StorageProto.*;
import storage.MetadataServiceGrpc.MetadataServiceImplBase;
import java.util.List;
import java.util.stream.Collectors;

public class MetadataGrpcService extends MetadataServiceImplBase {
    private final MetadataServer metadataServer;

    public MetadataGrpcService(MetadataServer metadataServer) {
        this.metadataServer = metadataServer;
    }

    @Override
    public void createFile(CreateFileRequest req, StreamObserver<FileMetadata> responseObserver) {
        try {
            // Delegate to existing metadata server logic
            MetadataServer.FileMetadata meta = metadataServer.createFile(
                req.getBucket(),
                req.getKey(),
                req.getOwner(),
                req.getSize(),
                req.getContentType()
            );
            responseObserver.onNext(toProtoFileMetadata(meta));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getFile(GetFileRequest req, StreamObserver<FileMetadata> responseObserver) {
        try {
            MetadataServer.FileMetadata meta = metadataServer.getFile(req.getBucket(), req.getKey());
            if (meta == null) {
                responseObserver.onError(
                    Status.NOT_FOUND.withDescription("File not found").asRuntimeException());
                return;
            }
            responseObserver.onNext(toProtoFileMetadata(meta));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void deleteFile(DeleteFileRequest req, StreamObserver<DeleteFileResponse> responseObserver) {
        try {
            boolean success = metadataServer.deleteFile(req.getBucket(), req.getKey());
            responseObserver.onNext(DeleteFileResponse.newBuilder().setSuccess(success).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void listFiles(ListFilesRequest req, StreamObserver<ListFilesResponse> responseObserver) {
        try {
            List<MetadataServer.FileMetadata> files = metadataServer.listFiles(req.getBucket());
            ListFilesResponse.Builder resp = ListFilesResponse.newBuilder();
            for (MetadataServer.FileMetadata f : files) {
                resp.addFiles(toProtoFileMetadata(f));
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getChunkLocations(GetChunkLocationsRequest req, StreamObserver<ChunkLocations> responseObserver) {
        try {
            List<MetadataServer.ChunkInfo> chunks = metadataServer.getChunkLocations(req.getBucket(), req.getKey());
            ChunkLocations.Builder resp = ChunkLocations.newBuilder();
            for (MetadataServer.ChunkInfo c : chunks) {
                resp.addChunks(ChunkInfo.newBuilder()
                    .setChunkId(c.chunkId)
                    .setOffset(c.offset)
                    .setSize(c.size)
                    .setStorageNode(c.storageNode)
                    .build());
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    // Helper to convert internal FileMetadata to protobuf
    private FileMetadata toProtoFileMetadata(MetadataServer.FileMetadata meta) {
        FileMetadata.Builder b = FileMetadata.newBuilder()
            .setBucket(meta.bucket)
            .setKey(meta.key)
            .setEtag(meta.etag)
            .setSize(meta.size)
            .setLastModified(meta.lastModified)
            .setOwner(meta.owner);
        for (MetadataServer.ChunkInfo c : meta.chunks) {
            b.addChunks(ChunkInfo.newBuilder()
                .setChunkId(c.chunkId)
                .setOffset(c.offset)
                .setSize(c.size)
                .setStorageNode(c.storageNode)
                .build());
        }
        return b.build();
    }
}
