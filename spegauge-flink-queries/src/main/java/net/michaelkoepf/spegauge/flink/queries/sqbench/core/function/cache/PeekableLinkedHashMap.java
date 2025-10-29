package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function.cache;
import java.util.*;
import java.util.function.Function;

/**
 * A HashMap-backed, doubly-linked map that mimics LinkedHashMap
 * (insertion- or access-order) and adds O(1) peek() that does not
 * change order. Iteration order is LRU -> MRU.
 */
public class PeekableLinkedHashMap<K,V> implements Map<K,V> {

    private final HashMap<K, Node<K,V>> index;
    private final boolean accessOrder;

    private Node<K,V> head; // LRU
    private Node<K,V> tail; // MRU

    private static final class Node<K,V> implements Map.Entry<K,V> {
        final K key;
        V value;
        Node<K,V> prev, next;

        Node(K key, V value) {
            this.key = key; this.value = value;
        }
        @Override public K getKey() { return key; }
        @Override public V getValue() { return value; }
        @Override public V setValue(V v) { V old = value; value = v; return old; }
    }

    public PeekableLinkedHashMap() {
        this(16, 0.75f, false);
    }
    public PeekableLinkedHashMap(int initialCapacity, float loadFactor, boolean accessOrder) {
        this.index = new HashMap<>(initialCapacity, loadFactor);
        this.accessOrder = accessOrder;
    }

    /* -------- Core helpers -------- */

    private void linkLast(Node<K,V> n) {
        Node<K,V> t = tail;
        tail = n;
        if (t == null) {
            head = n;
        } else {
            n.prev = t;
            t.next = n;
        }
    }

    private void unlink(Node<K,V> n) {
        Node<K,V> p = n.prev, q = n.next;
        if (p == null) head = q; else p.next = q;
        if (q == null) tail = p; else q.prev = p;
        n.prev = n.next = null;
    }

    private void moveToTail(Node<K,V> n) {
        if (tail == n) return;
        unlink(n);
        linkLast(n);
    }

    /* -------- Public API -------- */

    /** Like LinkedHashMap#get: returns value and, if accessOrder, moves entry to MRU. */
    @Override
    public V get(Object key) {
        Node<K,V> n = index.get(key);
        if (n == null) return null;
        if (accessOrder) moveToTail(n);
        return n.value;
    }

    /** O(1) lookup that does NOT change order. */
    public V peek(Object key) {
        Node<K,V> n = index.get(key);
        return n == null ? null : n.value;
    }

    /** Put new entry at MRU; if key exists, update value and (if accessOrder) move to MRU. */
    @Override
    public V put(K key, V value) {
        Node<K,V> n = index.get(key);
        if (n != null) {
            V old = n.value;
            n.value = value;
            if (accessOrder) moveToTail(n);
            return old;
        }
        Node<K,V> x = new Node<>(key, value);
        index.put(key, x);
        linkLast(x);
        return null;
    }

    @Override
    public V remove(Object key) {
        Node<K,V> n = index.remove(key);
        if (n == null) return null;
        unlink(n);
        return n.value;
    }

    @Override public boolean containsKey(Object key) { return index.containsKey(key); }
    @Override public int size() { return index.size(); }
    @Override public boolean isEmpty() { return index.isEmpty(); }

    @Override
    public void clear() {
        index.clear();
        head = tail = null;
    }

    /* -------- Iteration in LRU -> MRU order -------- */

    /** Iterator over entries in LRU -> MRU order (supports remove). */
    public Iterator<Map.Entry<K,V>> entryIteratorLRU() {
        return new Iterator<Map.Entry<K,V>>() {
            Node<K,V> next = head, lastRet = null;
            @Override public boolean hasNext() { return next != null; }
            @Override public Map.Entry<K,V> next() {
                if (next == null) throw new NoSuchElementException();
                lastRet = next;
                next = next.next;
                return lastRet;
            }
            @Override public void remove() {
                if (lastRet == null) throw new IllegalStateException();
                index.remove(lastRet.key);
                unlink(lastRet);
                lastRet = null;
            }
        };
    }

    /** Iterator over keys in LRU -> MRU order (supports remove). */
    public Iterator<K> keyIteratorLRU() {
        return new Iterator<K>() {
            Node<K,V> next = head, lastRet = null;
            @Override public boolean hasNext() { return next != null; }
            @Override public K next() {
                if (next == null) throw new NoSuchElementException();
                lastRet = next;
                next = next.next;
                return lastRet.key;
            }
            @Override public void remove() {
                if (lastRet == null) throw new IllegalStateException();
                index.remove(lastRet.key);
                unlink(lastRet);
                lastRet = null;
            }
        };
    }

    /* -------- Minimal Map interface plumbing -------- */

    @Override public boolean containsValue(Object value) {
        for (Node<K,V> n = head; n != null; n = n.next) {
            if (Objects.equals(n.value, value)) return true;
        }
        return false;
    }

    @Override public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) put(e.getKey(), e.getValue());
    }

    @Override
    public Set<K> keySet() {
        return index.keySet();
    }

    @Override
    public Collection<V> values() {
        ArrayList<V> vs = new ArrayList<>(size());
        for (Node<K,V> n = head; n != null; n = n.next) vs.add(n.value);
        return vs;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        // snapshot view; for streaming eviction prefer entryIteratorLRU()
        LinkedHashSet<Map.Entry<K,V>> es = new LinkedHashSet<>(size());
        for (Node<K,V> n = head; n != null; n = n.next) es.add(n);
        return es;
    }
}
