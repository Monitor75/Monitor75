import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;

public class MetadataServer {

    private static final int REPLICATION_FACTOR = 3;

    // Fixed group UUID — all peers must share this to form the same Raft group
    private static final UUID RAFT_GROUP_UUID =
            UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");

    // ---- Domain POJOs used by MetadataGrpcService ----

    public static class ChunkInfo {
        public final String       chunkId;
        public final long         offset;
        public final long         size;
        public final List<String> replicaNodes;
        public       String       storageNode;

        public ChunkInfo(String chunkId, long offset, long size,
                         List<String> replicaNodes) {
            this.chunkId      = chunkId;
            this.offset       = offset;
            this.size         = size;
            this.replicaNodes = new ArrayList<>(replicaNodes);
            this.storageNode  = replicaNodes.isEmpty() ? "" : replicaNodes.get(0);
        }
    }

    public static class FileMetadata {
        public final String          bucket;
        public final String          key;
        public final String          etag;
        public final long            size;
        public final String          lastModified;
        public final String          owner;
        public final List<ChunkInfo> chunks;

        public FileMetadata(String bucket, String key, String etag, long size,
                            String lastModified, String owner,
                            List<ChunkInfo> chunks) {
            this.bucket       = bucket;
            this.key          = key;
            this.etag         = etag;
            this.size         = size;
            this.lastModified = lastModified;
            this.owner        = owner;
            this.chunks       = chunks;
        }
    }

    // ---- JSON DTOs (plain public fields → Jackson-friendly without annotations) ----

    /** Serializable form of ChunkInfo for the Raft log. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ChunkDto {
        public String       chunkId      = "";
        public long         offset       = 0;
        public long         size         = 0;
        public String       storageNode  = "";
        public List<String> replicaNodes = new ArrayList<>();
    }

    /** Serializable form of FileMetadata for the Raft log. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FileMetadataDto {
        public String        bucket       = "";
        public String        key          = "";
        public String        etag         = "";
        public long          size         = 0;
        public String        lastModified = "";
        public String        owner        = "";
        public List<ChunkDto> chunks      = new ArrayList<>();
    }

    // ---- Fields ----

    private final ConsistentHash<String>  consistentHash;
    private final List<String>            dataNodes = new CopyOnWriteArrayList<>();
    private final EtcdClientWrapper       etcd;
    private final Server                  grpcServer;
    private final MetadataGrpcService     grpcService;
    private final RaftServer              raftServer;
    private final RaftClient              raftClient;
    private final MetadataStateMachine    stateMachine;
    private final ObjectMapper            mapper = new ObjectMapper();

    /**
     * @param etcdEndpoints  comma-separated etcd URLs
     * @param myAddr         this server's gRPC address, e.g. "localhost:50051"
     * @param grpcPort       gRPC listen port
     * @param nodeId         Raft peer ID, e.g. "node1"
     * @param raftAddr       Raft listen address, e.g. "localhost:6000"
     * @param peers          [id, raftAddr] pairs for ALL peers (including self)
     * @param initialNodes   pre-seeded storage node addresses (usually empty)
     */
    public MetadataServer(String etcdEndpoints, String myAddr, int grpcPort,
                          String nodeId, String raftAddr, List<String[]> peers,
                          List<String> initialNodes) throws Exception {
        this.consistentHash = new ConsistentHash<>(100, initialNodes);
        this.dataNodes.addAll(initialNodes);

        // etcd: register this metadata server and watch storage nodes
        this.etcd = new EtcdClientWrapper(etcdEndpoints, myAddr);
        this.etcd.registerMetadataServer();
        this.etcd.watchStorageNodes(
            nodeAddr -> {
                dataNodes.add(nodeAddr);
                consistentHash.add(nodeAddr);
                System.out.println("[cluster] Storage node joined: " + nodeAddr);
            },
            nodeAddr -> {
                dataNodes.remove(nodeAddr);
                consistentHash.remove(nodeAddr);
                System.out.println("[cluster] Storage node left: " + nodeAddr);
                onStorageNodeRemoved(nodeAddr);
            }
        );

        // Build Raft peer list
        List<RaftPeer> raftPeers = new ArrayList<>();
        for (String[] peer : peers) {
            raftPeers.add(RaftPeer.newBuilder()
                    .setId(peer[0]).setAddress(peer[1]).build());
        }
        RaftGroupId groupId = RaftGroupId.valueOf(RAFT_GROUP_UUID);
        RaftGroup   group   = RaftGroup.valueOf(groupId, raftPeers);

        RaftProperties raftProps = new RaftProperties();
        RaftConfigKeys.Rpc.setType(raftProps, SupportedRpcType.NETTY);
        // Use a workspace-relative persistent directory so Raft log and
        // RocksDB state survive container restarts / project sleeps.
        String dataRoot = System.getProperty("user.dir");   // /home/runner/workspace
        String raftStorageDir = dataRoot + "/data/raft-server-" + nodeId;
        String rocksDbDir     = dataRoot + "/data/raft-metadata-db-" + nodeId;
        new java.io.File(raftStorageDir).mkdirs();
        new java.io.File(rocksDbDir).mkdirs();

        raftProps.set("raft.server.storage.dir", raftStorageDir);
        // The Raft log directory is wiped by the workflow startup command so
        // Ratis always sees a clean directory and can FORMAT it successfully.
        // The actual file-metadata state lives in RocksDB (rocksDbDir above),
        // which is preserved across restarts; the StateMachine re-loads it on
        // startup via [state-machine] Loaded N entries from RocksDB.
        // Tell the Netty transport which port to bind the Raft server on.
        // Without this it defaults to port 0 (random), so peers can't find each other.
        int raftPort = Integer.parseInt(raftAddr.split(":")[1]);
        NettyConfigKeys.Server.setPort(raftProps, raftPort);

        // State machine — holds RocksDB + in-memory mirror
        this.stateMachine = new MetadataStateMachine(rocksDbDir);

        this.raftServer = RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf(nodeId))
                .setGroup(group)
                .setStateMachine(stateMachine)
                .setProperties(raftProps)
                .build();
        this.raftServer.start();
        System.out.println("[raft] Raft server started — node=" + nodeId
                + " addr=" + raftAddr);

        // Raft client — used to submit log entries for every write
        this.raftClient = RaftClient.newBuilder()
                .setRaftGroup(group)
                .setProperties(raftProps)
                .build();

        // gRPC metadata service
        this.grpcService = new MetadataGrpcService(this);
        this.grpcServer  = ServerBuilder.forPort(grpcPort)
                .addService(grpcService)
                .build()
                .start();
        System.out.println("[grpc] Metadata gRPC server started on port " + grpcPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            grpcServer.shutdown();
            try { raftClient.close(); } catch (Exception ignored) {}
            try { raftServer.close(); } catch (Exception ignored) {}
            etcd.close();
        }));
    }

    // ---- Raft write helper -----------------------------------------------

    /**
     * Submit a command through the Raft log and wait for it to be committed
     * by a majority.  Retries up to 10 times (with back-off) to handle the
     * window right after startup when a leader hasn't been elected yet.
     */
    private void raftSend(String command) throws Exception {
        Message msg = Message.valueOf(command);
        Exception last = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                RaftClientReply reply = raftClient.io().send(msg);
                if (reply.isSuccess()) return;
                // Non-successful reply (e.g. NOT_LEADER) — retry
                System.out.println("[raft] send attempt " + attempt
                        + " non-success: " + reply.getException());
            } catch (Exception e) {
                last = e;
                System.out.println("[raft] send attempt " + attempt
                        + " failed: " + e.getMessage());
            }
            Thread.sleep(300L * (1 << Math.min(attempt, 4))); // exponential back-off
        }
        throw new RuntimeException("Raft write failed after 10 attempts", last);
    }

    // ---- JSON serialization ----------------------------------------------

    /** Serialize a FileMetadata to the JSON DTO form stored in the Raft log. */
    private String toJson(FileMetadata meta) throws IOException {
        FileMetadataDto dto = new FileMetadataDto();
        dto.bucket       = meta.bucket;
        dto.key          = meta.key;
        dto.etag         = meta.etag;
        dto.size         = meta.size;
        dto.lastModified = meta.lastModified;
        dto.owner        = meta.owner;
        for (ChunkInfo c : meta.chunks) {
            ChunkDto cd      = new ChunkDto();
            cd.chunkId       = c.chunkId;
            cd.offset        = c.offset;
            cd.size          = c.size;
            cd.storageNode   = c.storageNode;
            cd.replicaNodes  = new ArrayList<>(c.replicaNodes);
            dto.chunks.add(cd);
        }
        return mapper.writeValueAsString(dto);
    }

    /** Deserialize a JSON string from the Raft log back to a FileMetadata. */
    private FileMetadata fromJson(String json) throws IOException {
        FileMetadataDto dto = mapper.readValue(json, FileMetadataDto.class);
        List<ChunkInfo> chunks = new ArrayList<>();
        for (ChunkDto cd : dto.chunks) {
            ChunkInfo ci = new ChunkInfo(cd.chunkId, cd.offset, cd.size,
                                         cd.replicaNodes);
            ci.storageNode = cd.storageNode;
            chunks.add(ci);
        }
        return new FileMetadata(dto.bucket, dto.key, dto.etag, dto.size,
                                dto.lastModified, dto.owner, chunks);
    }

    // ---- Business methods — all writes go through Raft -------------------

    public FileMetadata createFile(String bucket, String key, String owner,
                                   long size, String contentType) throws Exception {
        String chunkId  = UUID.randomUUID().toString();
        List<String> replicas = dataNodes.isEmpty()
                ? List.of("localhost:50052")
                : selectReplicaNodes(chunkId, dataNodes);
        ChunkInfo chunk = new ChunkInfo(chunkId, 0, size, replicas);
        FileMetadata meta = new FileMetadata(
                bucket, key,
                chunkId,
                size,
                Instant.now().toString(),
                owner,
                List.of(chunk)
        );

        // Serialize and commit through Raft — blocks until majority acknowledges
        String metaKey = bucket + "/" + key;
        String json    = toJson(meta);
        raftSend("PUT\t" + metaKey + "\t" + json);

        System.out.println("[meta] Committed " + metaKey
                + " via Raft -> replicas=" + replicas);
        return meta;
    }

    public FileMetadata getFile(String bucket, String key) {
        String metaKey = bucket + "/" + key;
        // When this node is a Raft follower the log entry may arrive a few
        // milliseconds after the client's raftSend() already returned.  Retry
        // briefly (up to 1 s) so the local state machine has time to catch up.
        String json = null;
        for (int attempt = 0; attempt < 20 && json == null; attempt++) {
            json = stateMachine.getJson(metaKey);
            if (json == null) {
                try { Thread.sleep(50); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (json == null) return null;
        try {
            return fromJson(json);
        } catch (IOException e) {
            System.err.println("[meta] Failed to deserialize " + metaKey
                    + ": " + e.getMessage());
            return null;
        }
    }

    public boolean deleteFile(String bucket, String key) {
        String metaKey = bucket + "/" + key;
        if (stateMachine.getJson(metaKey) == null) return false;
        try {
            raftSend("DEL\t" + metaKey);
            return true;
        } catch (Exception e) {
            System.err.println("[meta] Raft delete failed for " + metaKey
                    + ": " + e.getMessage());
            return false;
        }
    }

    public List<FileMetadata> listFiles(String bucket) {
        List<String> jsonList = stateMachine.listJsonByPrefix(bucket + "/");
        List<FileMetadata> result = new ArrayList<>();
        for (String json : jsonList) {
            try {
                result.add(fromJson(json));
            } catch (IOException e) {
                System.err.println("[meta] Skipping corrupt entry: " + e.getMessage());
            }
        }
        return result;
    }

    public List<ChunkInfo> getChunkLocations(String bucket, String key) {
        FileMetadata meta = getFile(bucket, key);
        return meta != null ? meta.chunks : new ArrayList<>();
    }

    // ---- Replication helpers ---------------------------------------------

    private List<String> selectReplicaNodes(String chunkId,
                                             List<String> availableNodes) {
        List<String> replicas = new ArrayList<>();
        String primary = consistentHash.get(chunkId);
        replicas.add(primary);
        for (int i = 1; i < REPLICATION_FACTOR && i < availableNodes.size(); i++) {
            String candidate = consistentHash.get(chunkId + "_replica" + i);
            if (!replicas.contains(candidate)) replicas.add(candidate);
        }
        return replicas;
    }

    private List<ChunkInfo> findChunksOnNode(String failedNode) {
        List<ChunkInfo> affected = new ArrayList<>();
        // Empty prefix matches every key in the state machine
        for (String json : stateMachine.listJsonByPrefix("")) {
            try {
                FileMetadata meta = fromJson(json);
                for (ChunkInfo chunk : meta.chunks) {
                    if (chunk.replicaNodes.contains(failedNode)) {
                        affected.add(chunk);
                        break;
                    }
                }
            } catch (IOException ignored) {}
        }
        return affected;
    }

    public void onStorageNodeRemoved(String failedNode) {
        List<ChunkInfo> affectedChunks = findChunksOnNode(failedNode);
        for (ChunkInfo chunk : affectedChunks) {
            String source = chunk.replicaNodes.stream()
                    .filter(n -> !n.equals(failedNode))
                    .findFirst().orElse(null);
            if (source == null) continue;
            String newTarget = consistentHash.get(chunk.chunkId + "_replica_new");
            if (!chunk.replicaNodes.contains(newTarget)
                    && !newTarget.equals(failedNode)) {
                replicateChunk(chunk.chunkId, source, newTarget);
                chunk.replicaNodes.remove(failedNode);
                chunk.replicaNodes.add(newTarget);
                chunk.storageNode = chunk.replicaNodes.get(0);
                System.out.println("[meta] Re-replicated " + chunk.chunkId
                        + " from " + source + " to " + newTarget);
            }
        }
    }

    private void replicateChunk(String chunkId, String sourceNode,
                                 String targetNode) {
        StorageNodeClient client = new StorageNodeClient(targetNode);
        try {
            client.replicateChunk(chunkId, sourceNode);
        } finally {
            client.close();
        }
    }

    // ---- Entry point -----------------------------------------------------

    /**
     * Usage: MetadataServer [nodeId] [raftAddr] [grpcPort] [etcdEndpoints]
     *                       [id@raftAddr ...]
     *
     * Example — three-node cluster (run once per node):
     *   MetadataServer node1 localhost:6000 50051 http://127.0.0.1:2379 \
     *                  node1@localhost:6000 node2@localhost:6001 node3@localhost:6002
     */
    public static void main(String[] args) throws Exception {
        String nodeId   = args.length > 0 ? args[0] : "node1";
        String raftAddr = args.length > 1 ? args[1] : "localhost:6000";
        int    grpcPort = args.length > 2 ? Integer.parseInt(args[2]) : 50051;
        String etcdEps  = args.length > 3 ? args[3]
                : "http://127.0.0.1:2379,http://127.0.0.1:2381,http://127.0.0.1:2383";

        List<String[]> peers = new ArrayList<>();
        for (int i = 4; i < args.length; i++) {
            String[] parts = args[i].split("@", 2);
            if (parts.length == 2) peers.add(parts);
        }
        if (peers.isEmpty()) {
            peers.add(new String[]{nodeId, raftAddr});
        }

        String myAddr = "localhost:" + grpcPort;
        System.out.println("[main] Starting MetadataServer node=" + nodeId
                + " raft=" + raftAddr + " grpc=" + grpcPort
                + " peers=" + peers.size());
        new MetadataServer(etcdEps, myAddr, grpcPort, nodeId, raftAddr,
                           peers, List.of());
        Thread.currentThread().join();
    }
}
