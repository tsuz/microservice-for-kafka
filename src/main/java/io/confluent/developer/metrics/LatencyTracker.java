// src/main/java/io/confluent/developer/metrics/LatencyTracker.java
package io.confluent.developer.metrics;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe latency tracker that maintains a sliding window of latencies
 * for percentile calculation
 */
public class LatencyTracker {
    private static final int WINDOW_SIZE = 10000; // Keep last 10k requests
    private final long[] latencies;
    private final AtomicLong writeIndex = new AtomicLong(0);
    private final AtomicLong requestCount = new AtomicLong(0);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    public LatencyTracker() {
        this.latencies = new long[WINDOW_SIZE];
        Arrays.fill(latencies, 0);
    }
    
    public void recordLatency(long latencyMs) {
        lock.writeLock().lock();
        try {
            int index = (int) (writeIndex.getAndIncrement() % WINDOW_SIZE);
            latencies[index] = latencyMs;
            requestCount.incrementAndGet();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public long getRequestCount() {
        return requestCount.get();
    }
    
    /**
     * Calculate percentile from recorded latencies
     * @param percentile Value between 0 and 100 (e.g., 90 for 90th percentile)
     * @return Latency at the given percentile in milliseconds
     */
    public double getPercentile(double percentile) {
        lock.readLock().lock();
        try {
            long count = requestCount.get();
            if (count == 0) {
                return 0.0;
            }
            
            // Copy and sort only the valid entries
            int validSize = (int) Math.min(count, WINDOW_SIZE);
            long[] sorted = new long[validSize];
            System.arraycopy(latencies, 0, sorted, 0, validSize);
            Arrays.sort(sorted);
            
            // Calculate percentile index
            int index = (int) Math.ceil(percentile / 100.0 * validSize) - 1;
            index = Math.max(0, Math.min(index, validSize - 1));
            
            return sorted[index];
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public double getP50() {
        return getPercentile(50.0);
    }

    public double getP90() {
        return getPercentile(90.0);
    }

    public double getP95() {
        return getPercentile(95.0);
    }

    public double getP99() {
        return getPercentile(99.0);
    }
    
    public double getP999() {
        return getPercentile(99.9);
    }
}