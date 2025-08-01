package group.mail.models;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A thread-safe, application-scoped singleton that holds the real-time status
 * and collected metrics of the data ingestion process.
 */
@Component
public class IngestStatus {

    /**
     * Data carrier for returning top sender statistics.
     */
    public record SenderStat(String email, long count) {}

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

    // --- Process State Fields ---
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<Instant> startTime = new AtomicReference<>();
    private final AtomicReference<Instant> endTime = new AtomicReference<>();
    private final AtomicReference<IngestionPhase> currentPhase = new AtomicReference<>(IngestionPhase.IDLE);
    private final AtomicReference<String> errorMessage = new AtomicReference<>();

    // --- Metrics Fields (previously IngestionMetricsData) ---
    private final AtomicLong processedFileCount = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> senderCounts = new ConcurrentHashMap<>();


    /**
     * Resets all state and metrics to their initial values and marks the process as started.
     * This should be called once at the beginning of a new ingestion run.
     */
    public void start() {
        // Reset state
        endTime.set(null);
        errorMessage.set(null);
        startTime.set(Instant.now());
        currentPhase.set(IngestionPhase.IDLE);
        running.set(true);

        processedFileCount.set(0);
        senderCounts.clear();
    }

    /**
     * Marks the process as successfully finished.
     */
    public void finish() {
        endTime.set(Instant.now());
        setPhase(IngestionPhase.COMPLETED);
        running.set(false);
    }

    /**
     * Marks the process as failed, capturing the error details.
     * @param ex The exception that caused the failure.
     */
    public void fail(Throwable ex) {
        endTime.set(Instant.now());
        errorMessage.set(ex.getMessage());
        setPhase(IngestionPhase.FAILED);
        running.set(false);
    }

    /**
     * Atomically records a single processed file and updates the sender's count.
     * This method is designed for high-concurrency calls from worker threads.
     *
     * @param senderEmail The email address of the file's sender.
     */
    public void recordFile(String senderEmail) {
        processedFileCount.incrementAndGet();
        if (senderEmail != null && !senderEmail.isBlank()) {
            senderCounts.computeIfAbsent(senderEmail, k -> new AtomicLong(0)).incrementAndGet();
        }
    }

    /**
     * Increments the total count of processed files, used when the sender is not available.
     */
    public void incrementProcessedFileCount() {
        processedFileCount.incrementAndGet();
    }

    public void setPhase(IngestionPhase phase) {
        this.currentPhase.set(phase);
    }

    // --- Getters for State and Metrics ---

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

    /**
     * Calculates the duration of the ingestion process.
     * @return The duration, or zero if the process hasn't started.
     */
    public Duration getDuration() {
        Instant start = startTime.get();
        if (start == null) {
            return Duration.ZERO;
        }
        Instant end = endTime.get() != null ? endTime.get() : Instant.now();
        return Duration.between(start, end);
    }

    /**
     * Calculates and returns a list of the top senders by message count.
     *
     * @param limit The maximum number of top senders to return.
     * @return A sorted list of {@link SenderStat} objects.
     */
    public List<SenderStat> getTopSenders(int limit) {
        if (limit <= 0) {
            return Collections.emptyList();
        }
        return senderCounts.entrySet().stream()
                .sorted(Map.Entry.<String, AtomicLong>comparingByValue(Comparator.comparingLong(AtomicLong::get)).reversed())
                .limit(limit)
                .map(entry -> new SenderStat(entry.getKey(), entry.getValue().get()))
                .collect(Collectors.toList());
    }
}