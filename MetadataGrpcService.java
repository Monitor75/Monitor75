import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class EtcdClientWrapper {
    private static final String METADATA_KEY_PREFIX = "/services/metadata/";
    private static final String STORAGE_KEY_PREFIX = "/services/storage/";
    private final Client client;
    private final String myAddr;
    private long leaseId;
    private ScheduledExecutorService keepAliveExecutor;

    public EtcdClientWrapper(String endpoints, String myAddr) {
        this.client = Client.builder().endpoints(endpoints.split(",")).build();
        this.myAddr = myAddr;
    }

    public void registerMetadataServer() throws Exception {
        // Grant a lease (10 seconds TTL)
        System.out.println("[etcd] Requesting lease...");
        System.out.flush();
        leaseId = client.getLeaseClient().grant(10).get(5, TimeUnit.SECONDS).getID();
        System.out.println("[etcd] Lease granted: " + leaseId);
        System.out.flush();
        // Put key with lease
        ByteSequence key = ByteSequence.from(METADATA_KEY_PREFIX + myAddr, StandardCharsets.UTF_8);
        ByteSequence value = ByteSequence.from(myAddr, StandardCharsets.UTF_8);
        client.getKVClient().put(key, value).get(5, TimeUnit.SECONDS);
        System.out.println("[etcd] Registered: " + METADATA_KEY_PREFIX + myAddr);
        System.out.flush();

        // Keep lease alive
        keepAliveExecutor = Executors.newSingleThreadScheduledExecutor();
        keepAliveExecutor.scheduleAtFixedRate(() -> {
            try {
                client.getLeaseClient().keepAliveOnce(leaseId).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    public void watchStorageNodes(Consumer<String> onNodeAdded, Consumer<String> onNodeRemoved) {
        Watch.Watcher watcher = client.getWatchClient().watch(
            ByteSequence.from(STORAGE_KEY_PREFIX, StandardCharsets.UTF_8),
            new Watch.Listener() {
                @Override
                public void onNext(WatchResponse response) {
                    response.getEvents().forEach(event -> {
                        String key = event.getKeyValue().getKey().toString(StandardCharsets.UTF_8);
                        String addr = event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                        if (event.getEventType() == WatchEvent.EventType.PUT) {
                            onNodeAdded.accept(addr);
                        } else if (event.getEventType() == WatchEvent.EventType.DELETE) {
                            onNodeRemoved.accept(addr);
                        }
                    });
                }

                @Override
                public void onError(Throwable throwable) {}

                @Override
                public void onCompleted() {}
            });
        // Keep watcher reference if needed; here we ignore for simplicity
    }

    public void close() {
        keepAliveExecutor.shutdown();
        try {
            client.getLeaseClient().revoke(leaseId).get();
        } catch (Exception ignored) {}
        client.close();
    }
}
