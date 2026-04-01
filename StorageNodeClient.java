import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Raft state machine backed by RocksDB + an in-memory mirror cache.
 *
 * Wire format for log entries (tab-delimited so JSON values can contain colons):
 *   PUT\t<metaKey>\t<jsonValue>   — upsert a file-metadata record
 *   DEL\t<metaKey>               — remove a file-metadata record
 *
 * <metaKey> = bucket + "/" + key
 *
 * applyTransaction writes to both RocksDB (durable) and memCache (fast reads).
 * All three Raft nodes apply every committed log entry, so their memCaches stay in sync.
 */
public class MetadataStateMachine extends BaseStateMachine {

    static { RocksDB.loadLibrary(); }

    private final RocksDB       db;
    private final WriteOptions  writeOptions;
    private final String        snapshotBase;
    private final ObjectMapper  mapper = new ObjectMapper();

    // In-memory mirror — always consistent with committed Raft log entries.
    // Key: bucket+"/"+key   Value: raw JSON string
    private final ConcurrentHashMap<String, String> memCache = new ConcurrentHashMap<>();

    public MetadataStateMachine(String dbPath) throws RocksDBException {
        Options options = new Options().setCreateIfMissing(true);
        this.db           = RocksDB.open(options, dbPath);
        this.writeOptions = new WriteOptions().setSync(true);
        this.snapshotBase = dbPath + "-snapshots";
        new File(snapshotBase).mkdirs();

        // Warm the in-memory cache from whatever was already persisted in RocksDB
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                String k = new String(it.key(),   StandardCharsets.UTF_8);
                String v = new String(it.value(), StandardCharsets.UTF_8);
                memCache.put(k, v);
            }
        }
        System.out.println("[state-machine] Loaded " + memCache.size()
                + " entries from RocksDB into memory");
    }

    // ---- Snapshot support ------------------------------------------------

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId,
                           RaftStorage storage) throws IOException {
        super.initialize(server, groupId, storage);
    }

    @Override
    public long takeSnapshot() throws IOException {
        final long index = getLastAppliedTermIndex().getIndex();
        if (index < 0) return index;
        String snapshotPath = snapshotBase + "/checkpoint_" + index;
        try {
            Checkpoint cp = Checkpoint.create(db);
            cp.createCheckpoint(snapshotPath);
            System.out.println("[state-machine] Snapshot at index " + index
                    + " -> " + snapshotPath);
            pruneOldSnapshots(index);
            return index;
        } catch (RocksDBException e) {
            throw new IOException("RocksDB checkpoint failed at index " + index, e);
        }
    }

    private void pruneOldSnapshots(long currentIndex) {
        File dir = new File(snapshotBase);
        File[] files = dir.listFiles(f ->
                f.isDirectory() && f.getName().startsWith("checkpoint_"));
        if (files == null) return;
        for (File f : files) {
            try {
                long idx = Long.parseLong(f.getName().replace("checkpoint_", ""));
                if (idx < currentIndex - 1) deleteRecursive(f);
            } catch (NumberFormatException ignored) {}
        }
    }

    private void deleteRecursive(File f) {
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) for (File c : children) deleteRecursive(c);
        }
        f.delete();
    }

    // ---- Log application -------------------------------------------------

    /**
     * Called by Raft once a log entry has been committed by a majority of nodes.
     * Updates both RocksDB (durable) and memCache (fast).
     * MUST call updateLastAppliedTermIndex so Raft can truncate old log entries.
     */
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final long term  = trx.getLogEntry().getTerm();
        final long index = trx.getLogEntry().getIndex();

        // Tab-delimited: op \t key [\t value]
        String raw = trx.getStateMachineLogEntry().getLogData().toStringUtf8();
        int t1 = raw.indexOf('\t');
        int t2 = (t1 >= 0) ? raw.indexOf('\t', t1 + 1) : -1;

        try {
            if (t1 > 0) {
                String op  = raw.substring(0, t1);
                String key = (t2 > t1) ? raw.substring(t1 + 1, t2)
                                        : raw.substring(t1 + 1);

                if ("PUT".equals(op) && t2 > t1) {
                    String value = raw.substring(t2 + 1);
                    db.put(writeOptions,
                            key.getBytes(StandardCharsets.UTF_8),
                            value.getBytes(StandardCharsets.UTF_8));
                    memCache.put(key, value);

                } else if ("DEL".equals(op)) {
                    db.delete(writeOptions, key.getBytes(StandardCharsets.UTF_8));
                    memCache.remove(key);
                }
            }
            updateLastAppliedTermIndex(term, index);
            return CompletableFuture.completedFuture(Message.valueOf("OK"));
        } catch (RocksDBException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // ---- Read API (used by MetadataServer for all reads) -----------------

    /** Returns the raw JSON for bucket/key, or null if not found. */
    public String getJson(String metaKey) {
        return memCache.get(metaKey);
    }

    /** Returns all JSON values whose key starts with the given prefix. */
    public List<String> listJsonByPrefix(String prefix) {
        return memCache.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    /** Returns snapshot of all entries — for debugging/admin queries. */
    public int size() {
        return memCache.size();
    }

    // ---- Legacy Raft query (read-path via Raft protocol, rarely used) ----

    @Override
    public CompletableFuture<Message> query(Message request) {
        String key   = request.getContent().toStringUtf8();
        String value = memCache.get(key);
        ByteString result = (value == null) ? ByteString.EMPTY
                : ByteString.copyFromUtf8(value);
        return CompletableFuture.completedFuture(Message.valueOf(result));
    }

    @Override
    public void close() throws IOException {
        if (writeOptions != null) writeOptions.close();
        if (db != null)           db.close();
    }
}
