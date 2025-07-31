package group.mail.models;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A thread-safe, application-scoped component for collecting and providing
 * metrics about the ingestion process, such as file counts and sender statistics.
 */
@Component
public class IngestionMetricsData {

    private final AtomicLong processedFileCount = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> senderCounts = new ConcurrentHashMap<>();

    /**
     * Data carrier for returning top sender statistics.
     */
    public record SenderStat(String email, long count) {}

    /**
     * Resets all metrics to their initial state. Should be called at the
     * beginning of a new ingestion process.
     */
    public void reset() {
        processedFileCount.set(0);
        senderCounts.clear();
    }

    /**
     * Atomically records a single processed file and updates the sender's count.
     * This method is designed for high-concurrency calls from worker threads.
     *
     * @param senderEmail The email address of the file's sender. Can be null or blank.
     */
    public void recordFile(String senderEmail) {
        if (senderEmail != null && !senderEmail.isBlank()) {
            processedFileCount.incrementAndGet();
            senderCounts.computeIfAbsent(senderEmail, k -> new AtomicLong(0)).incrementAndGet();
        }
    }

    public void incrementProcessedFileCount() {
        processedFileCount.incrementAndGet();
    }

    /**
     * Gets the total number of files processed since the last reset.
     * @return The total count of processed files.
     */
    public long getProcessedFileCount() {
        return processedFileCount.get();
    }

    /**
     * Calculates and returns a list of the top senders by message count.
     *
     * @param limit The maximum number of top senders to return.
     * @return A list of {@link SenderStat} objects, sorted by message count in descending order.
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