import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Consistent hashing ring for distributing chunks across storage nodes.
 * @param <T> type of node (e.g., String node id)
 */
public class ConsistentHash<T> {
    private final int numberOfReplicas; // virtual nodes per physical node
    private final SortedMap<Long, T> circle = new TreeMap<>();
    private final MessageDigest md;

    public ConsistentHash(int numberOfReplicas, Collection<T> nodes) throws NoSuchAlgorithmException {
        this.numberOfReplicas = numberOfReplicas;
        this.md = MessageDigest.getInstance("MD5");
        for (T node : nodes) {
            add(node);
        }
    }

    public void add(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            long hash = hash(node.toString() + i);
            circle.put(hash, node);
        }
    }

    public void remove(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            long hash = hash(node.toString() + i);
            circle.remove(hash);
        }
    }

    /**
     * Returns the node responsible for the given key (chunk id).
     */
    public T get(Object key) {
        if (circle.isEmpty()) return null;
        long hash = hash(key.toString());
        if (!circle.containsKey(hash)) {
            SortedMap<Long, T> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    private long hash(String key) {
        md.reset();
        md.update(key.getBytes());
        byte[] digest = md.digest();
        long h = 0;
        for (int i = 0; i < 4; i++) {
            h <<= 8;
            h |= ((int) digest[i]) & 0xFF;
        }
        return h;
    }
}
