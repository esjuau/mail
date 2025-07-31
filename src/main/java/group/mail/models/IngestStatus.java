package group.mail.models;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A thread-safe, application-scoped singleton that holds the real-time status 
 * of the data ingestion process.
 */
public class IngestStatus {

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong processedFileCount = new AtomicLong(0);
    private final AtomicReference<Instant> startTime = new AtomicReference<>();
    private final AtomicReference<Instant> endTime = new AtomicReference<>();

    /**
     * Resets the status and marks the ingestion process as started.
     * This should be called once at the beginning of the ingestion pipeline.
     */
    public void start() {
        processedFileCount.set(0);
        endTime.set(null);
        startTime.set(Instant.now());
        running.set(true);
    }

    /**
     * Marks the ingestion process as finished.
     * This should be called once at the very end of the pipeline (in success or failure).
     */
    public void finish() {
        endTime.set(Instant.now());
        running.set(false);
    }

    /**
     * Atomically increments the counter for processed files.
     * This method is designed to be called concurrently from multiple worker threads.
     */
    public void fileProcessed() {
        processedFileCount.incrementAndGet();
    }

    public boolean isRunning() {
        return running.get();
    }

    public long getProcessedFileCount() {
        return processedFileCount.get();
    }

    public Instant getStartTime() {
        return startTime.get();
    }

    public Instant getEndTime() {
        return endTime.get();
    }

    /**
     * Calculates the duration of the ingestion process.
     * @return The duration between start and end time. If the process is still running,
     * returns duration between start time and now. If not started, returns zero.
     */
    public Duration getDuration() {
        Instant start = startTime.get();
        if (start == null) {
            return Duration.ZERO;
        }
        Instant end = endTime.get() != null ? endTime.get() : Instant.now();
        return Duration.between(start, end);
    }
}