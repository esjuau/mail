package group.mail.models;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A thread-safe, application-scoped singleton that holds the real-time status 
 * of the data ingestion process.
 */
@Component
public class IngestStatus {

    /**
     * Represents the distinct phases of the ingestion pipeline.
     */
    public enum IngestionPhase {
        IDLE,
        DOWNLOADING,
        EXTRACTING,
        PROCESSING,
        CLEANING_UP,
        COMPLETED,
        FAILED
    }

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong processedFileCount = new AtomicLong(0);
    private final AtomicReference<Instant> startTime = new AtomicReference<>();
    private final AtomicReference<Instant> endTime = new AtomicReference<>();
    private final AtomicReference<IngestionPhase> currentPhase = new AtomicReference<>(IngestionPhase.IDLE);
    private final AtomicReference<String> errorMessage = new AtomicReference<>();

    public void start() {
        processedFileCount.set(0);
        endTime.set(null);
        errorMessage.set(null);
        startTime.set(Instant.now());
        currentPhase.set(IngestionPhase.IDLE);
        running.set(true);
    }
    
    public void finish() {
        endTime.set(Instant.now());
        setPhase(IngestionPhase.COMPLETED);
        running.set(false);
    }

    public void fail(Throwable ex) {
        endTime.set(Instant.now());
        errorMessage.set(ex.getMessage());
        setPhase(IngestionPhase.FAILED);
        running.set(false);
    }

    public void fileProcessed() {
        processedFileCount.incrementAndGet();
    }
    
    public void setPhase(IngestionPhase phase) {
        this.currentPhase.set(phase);
    }
    
    public IngestionPhase getPhase() {
        return currentPhase.get();
    }

    public boolean isRunning() {
        return running.get();
    }

    public long getProcessedFileCount() {
        return processedFileCount.get();
    }
    
    public String getErrorMessage() {
        return errorMessage.get();
    }

    public Instant getStartTime() {
        return startTime.get();
    }

    public Instant getEndTime() {
        return endTime.get();
    }

    public Duration getDuration() {
        Instant start = startTime.get();
        if (start == null) {
            return Duration.ZERO;
        }
        Instant end = endTime.get() != null ? endTime.get() : Instant.now();
        return Duration.between(start, end);
    }
}