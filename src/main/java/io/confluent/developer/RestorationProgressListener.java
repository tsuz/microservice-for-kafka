package io.confluent.developer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Listener to track and log the restoration progress of Kafka Streams state stores.
 * Progress logs are simplified, while completion logs include detailed throughput metrics.
 */
public class RestorationProgressListener implements StateRestoreListener {
    
    private static final Logger logger = LoggerFactory.getLogger(RestorationProgressListener.class);
    
    // Track restoration progress per store
    private final Map<String, StoreRestorationInfo> restorationInfo = new ConcurrentHashMap<>();
    
    // Average bytes per record (will be calculated from actual data)
    private static final int DEFAULT_BYTES_PER_RECORD = 1024; // 1KB default estimate
    
    private static class StoreRestorationInfo {
        final String storeName;
        final long startingOffset;
        final long endingOffset;
        final long totalRecords;
        final long startTimeMs;
        long recordsRestored;
        long totalBytesRestored;
        long lastLoggedPercentage;
        
        // For calculating instantaneous throughput
        long lastBatchTimeMs;
        long lastBatchRecords;
        long lastBatchBytes;
        
        // Moving average for smoother metrics
        double avgRecordsPerSec;
        double avgBytesPerSec;
        
        StoreRestorationInfo(String storeName, long startingOffset, long endingOffset) {
            this.storeName = storeName;
            this.startingOffset = startingOffset;
            this.endingOffset = endingOffset;
            this.totalRecords = endingOffset - startingOffset;
            this.startTimeMs = System.currentTimeMillis();
            this.recordsRestored = 0;
            this.totalBytesRestored = 0;
            this.lastLoggedPercentage = 0;
            this.lastBatchTimeMs = startTimeMs;
            this.lastBatchRecords = 0;
            this.lastBatchBytes = 0;
            this.avgRecordsPerSec = 0;
            this.avgBytesPerSec = 0;
        }
        
        void addRestored(long count, long bytes) {
            long currentTimeMs = System.currentTimeMillis();
            
            this.recordsRestored += count;
            this.totalBytesRestored += bytes;
            
            // Calculate instantaneous throughput for this batch
            long timeDeltaMs = currentTimeMs - lastBatchTimeMs;
            if (timeDeltaMs > 0) {
                double batchRecordsPerSec = (count * 1000.0) / timeDeltaMs;
                double batchBytesPerSec = (bytes * 1000.0) / timeDeltaMs;
                
                // Update moving average (70% old + 30% new for smoothing)
                if (avgRecordsPerSec == 0) {
                    avgRecordsPerSec = batchRecordsPerSec;
                    avgBytesPerSec = batchBytesPerSec;
                } else {
                    avgRecordsPerSec = (avgRecordsPerSec * 0.7) + (batchRecordsPerSec * 0.3);
                    avgBytesPerSec = (avgBytesPerSec * 0.7) + (batchBytesPerSec * 0.3);
                }
            }
            
            this.lastBatchTimeMs = currentTimeMs;
            this.lastBatchRecords = count;
            this.lastBatchBytes = bytes;
        }
        
        double getProgressPercentage() {
            if (totalRecords == 0) return 100.0;
            return (recordsRestored * 100.0) / totalRecords;
        }
        
        long getElapsedTimeMs() {
            return System.currentTimeMillis() - startTimeMs;
        }
        
        long getEstimatedRemainingTimeMs() {
            if (recordsRestored == 0) return -1;
            
            long elapsedMs = getElapsedTimeMs();
            double recordsPerMs = (double) recordsRestored / elapsedMs;
            long remainingRecords = totalRecords - recordsRestored;
            
            return (long) (remainingRecords / recordsPerMs);
        }
        
        String getEstimatedRemainingTimeFormatted() {
            long remainingMs = getEstimatedRemainingTimeMs();
            if (remainingMs < 0) return "calculating...";
            
            long seconds = remainingMs / 1000;
            long minutes = seconds / 60;
            long hours = minutes / 60;
            
            if (hours > 0) {
                return String.format("%dh %dm %ds", hours, minutes % 60, seconds % 60);
            } else if (minutes > 0) {
                return String.format("%dm %ds", minutes, seconds % 60);
            } else {
                return String.format("%ds", seconds);
            }
        }
        
        double getOverallRecordsPerSec() {
            long elapsedSeconds = getElapsedTimeMs() / 1000;
            if (elapsedSeconds == 0) return 0;
            return recordsRestored / (double) elapsedSeconds;
        }
        
        double getOverallBytesPerSec() {
            long elapsedSeconds = getElapsedTimeMs() / 1000;
            if (elapsedSeconds == 0) return 0;
            return totalBytesRestored / (double) elapsedSeconds;
        }
        
        String formatBytesPerSec(double bytesPerSec) {
            if (bytesPerSec >= 1024 * 1024 * 1024) {
                return String.format("%.2f GB/s", bytesPerSec / (1024 * 1024 * 1024));
            } else if (bytesPerSec >= 1024 * 1024) {
                return String.format("%.2f MB/s", bytesPerSec / (1024 * 1024));
            } else if (bytesPerSec >= 1024) {
                return String.format("%.2f KB/s", bytesPerSec / 1024);
            } else {
                return String.format("%.0f B/s", bytesPerSec);
            }
        }
        
        String formatBytes(long bytes) {
            if (bytes >= 1024L * 1024 * 1024) {
                return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
            } else if (bytes >= 1024L * 1024) {
                return String.format("%.2f MB", bytes / (1024.0 * 1024));
            } else if (bytes >= 1024L) {
                return String.format("%.2f KB", bytes / 1024.0);
            } else {
                return String.format("%d B", bytes);
            }
        }
        
        int getAvgBytesPerRecord() {
            if (recordsRestored == 0) return DEFAULT_BYTES_PER_RECORD;
            return (int) (totalBytesRestored / recordsRestored);
        }
        
        String getKey() {
            return storeName;
        }
    }

    @Override
    public void onRestoreStart(TopicPartition topicPartition, 
                              String storeName, 
                              long startingOffset, 
                              long endingOffset) {
        
        StoreRestorationInfo info = new StoreRestorationInfo(storeName, startingOffset, endingOffset);
        restorationInfo.put(info.getKey(), info);
        
        logger.info("=================================================================");
        logger.info("RESTORATION STARTED");
        logger.info("=================================================================");
        logger.info("Store Name:        {}", storeName);
        logger.info("Topic Partition:   {}", topicPartition);
        logger.info("Starting Offset:   {}", startingOffset);
        logger.info("Ending Offset:     {}", endingOffset);
        logger.info("Total Records:     {}", info.totalRecords);
        logger.info("=================================================================");
    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition,
                               String storeName,
                               long batchEndOffset,
                               long numRestored) {
        
        String key = storeName;
        StoreRestorationInfo info = restorationInfo.get(key);
        
        if (info == null) {
            logger.warn("Received batch restored event for unknown store: {}", storeName);
            return;
        }
        
        // Estimate bytes for this batch
        // We use the current average or default if not available yet
        long estimatedBytes = numRestored * info.getAvgBytesPerRecord();
        
        info.addRestored(numRestored, estimatedBytes);
        
        double currentProgress = info.getProgressPercentage();
        
        // Log every 10% progress or if it's the first batch
        if (shouldLogProgress(info, currentProgress)) {
            long elapsedSeconds = info.getElapsedTimeMs() / 1000;
            String remainingTime = info.getEstimatedRemainingTimeFormatted();
            
            // SIMPLIFIED PROGRESS LOGS (no throughput metrics)
            logger.info("-----------------------------------------------------------------");
            logger.info("RESTORATION PROGRESS: {}", storeName);
            logger.info("-----------------------------------------------------------------");
            logger.info("Progress:           {}/{} records ({}%)", 
                       info.recordsRestored, info.totalRecords, String.format("%.2f", currentProgress));
            logger.info("Data Restored:      {}", info.formatBytes(info.totalBytesRestored));
            logger.info("Batch End Offset:   {}", batchEndOffset);
            logger.info("Elapsed Time:       {}s", elapsedSeconds);
            logger.info("Est. Time Left:     {}", remainingTime);
            logger.info("-----------------------------------------------------------------");
            
            info.lastLoggedPercentage = (long) currentProgress;
        }
    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition,
                            String storeName,
                            long totalRestored) {
        
        String key = storeName;
        StoreRestorationInfo info = restorationInfo.get(key);
        
        if (info == null) {
            logger.warn("Received restore end event for unknown store: {}", storeName);
            return;
        }
        
        long totalTimeMs = info.getElapsedTimeMs();
        long totalTimeSeconds = totalTimeMs / 1000;
        double recordsPerSecond = totalRestored / Math.max(1.0, totalTimeSeconds);
        double bytesPerSecond = info.totalBytesRestored / Math.max(1.0, totalTimeSeconds);
        
        // DETAILED COMPLETION LOGS (includes all throughput metrics)
        logger.info("=================================================================");
        logger.info("RESTORATION COMPLETED");
        logger.info("=================================================================");
        logger.info("Store Name:         {}", storeName);
        logger.info("Topic Partition:    {}", topicPartition);
        logger.info("Total Restored:     {} records", totalRestored);
        logger.info("Total Data Size:    {}", info.formatBytes(info.totalBytesRestored));
        logger.info("Avg Record Size:    {} bytes", info.getAvgBytesPerRecord());
        logger.info("Total Time:         {}s ({} minutes)", 
                   totalTimeSeconds, totalTimeSeconds / 60);
        logger.info("-----------------------------------------------------------------");
        logger.info("THROUGHPUT METRICS:");
        logger.info("  Records/sec:      {} records/sec", String.format("%.0f", recordsPerSecond));
        logger.info("  Data Rate:        {}", info.formatBytesPerSec(bytesPerSecond));
        logger.info("=================================================================");
        
        // Clean up
        restorationInfo.remove(key);
    }
    
    /**
     * Determine if we should log progress at this point.
     * Logs every 10% or on the first batch.
     */
    private boolean shouldLogProgress(StoreRestorationInfo info, double currentProgress) {
        long currentPercentageBucket = (long) (currentProgress / 10) * 10;
        return currentPercentageBucket > info.lastLoggedPercentage || info.recordsRestored == 0;
    }
    
    /**
     * Get current restoration status for all stores.
     * Useful for exposing via REST API or monitoring.
     */
    public Map<String, RestorationStatus> getRestorationStatus() {
        Map<String, RestorationStatus> status = new HashMap<>();
        
        for (Map.Entry<String, StoreRestorationInfo> entry : restorationInfo.entrySet()) {
            StoreRestorationInfo info = entry.getValue();
            status.put(entry.getKey(), new RestorationStatus(
                info.storeName,
                info.recordsRestored,
                info.totalRecords,
                info.totalBytesRestored,
                info.getProgressPercentage(),
                info.getElapsedTimeMs(),
                info.getEstimatedRemainingTimeMs(),
                info.getOverallRecordsPerSec(),
                info.getOverallBytesPerSec(),
                info.avgRecordsPerSec,
                info.avgBytesPerSec
            ));
        }
        
        return status;
    }
    
    /**
     * Check if any restoration is currently in progress
     */
    public boolean isRestorationInProgress() {
        return !restorationInfo.isEmpty();
    }
    
    /**
     * Status object for external consumption
     */
    public static class RestorationStatus {
        public final String storeName;
        public final long recordsRestored;
        public final long totalRecords;
        public final long bytesRestored;
        public final double progressPercentage;
        public final long elapsedTimeMs;
        public final long estimatedRemainingTimeMs;
        public final double overallRecordsPerSec;
        public final double overallBytesPerSec;
        public final double currentRecordsPerSec;
        public final double currentBytesPerSec;
        
        public RestorationStatus(String storeName, long recordsRestored, long totalRecords,
                               long bytesRestored, double progressPercentage, long elapsedTimeMs, 
                               long estimatedRemainingTimeMs, double overallRecordsPerSec,
                               double overallBytesPerSec, double currentRecordsPerSec,
                               double currentBytesPerSec) {
            this.storeName = storeName;
            this.recordsRestored = recordsRestored;
            this.totalRecords = totalRecords;
            this.bytesRestored = bytesRestored;
            this.progressPercentage = progressPercentage;
            this.elapsedTimeMs = elapsedTimeMs;
            this.estimatedRemainingTimeMs = estimatedRemainingTimeMs;
            this.overallRecordsPerSec = overallRecordsPerSec;
            this.overallBytesPerSec = overallBytesPerSec;
            this.currentRecordsPerSec = currentRecordsPerSec;
            this.currentBytesPerSec = currentBytesPerSec;
        }
    }
}