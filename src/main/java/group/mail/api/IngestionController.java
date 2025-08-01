package group.mail.api;

import group.mail.models.IngestStatus;
import group.mail.services.DatasetIngestionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class IngestionController {

    private final DatasetIngestionService ingestionService;
    private final IngestStatus ingestStatus;

    /**
     * Starts the asynchronous data ingestion process.
     * @return A response entity indicating that the process has started.
     */
    @PostMapping("/start")
    public ResponseEntity<Map<String, String>> startIngestion() {
        Map<String, String> response = new HashMap<>();
        try {
            ingestionService.startIngestion();
            response.put("message", "Ingestion process started successfully.");
            return ResponseEntity.ok(response);
        } catch (IllegalStateException e) {
            response.put("message", "Failed to start ingestion: " + e.getMessage());
            // Use 409 Conflict for "already running" errors
            return ResponseEntity.status(409).body(response);
        } catch (Exception e) {
            response.put("message", "An unexpected error occurred while starting ingestion.");
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * Retrieves the current status of the data ingestion process.
     * This includes the current phase, whether it's running, and processing metrics.
     * @return A map containing the detailed status.
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getIngestionStatus() {
        try {
            Map<String, Object> statusMap = new LinkedHashMap<>();

            // Overall process status
            statusMap.put("running", ingestStatus.isRunning());
            statusMap.put("phase", ingestStatus.getPhase());

            // Timing information
            statusMap.put("startTime", ingestStatus.getStartTime());
            statusMap.put("durationSeconds", ingestStatus.getDuration().toSeconds());

            // Processing metrics from the centralized status object
            statusMap.put("filesProcessed", ingestStatus.getProcessedFileCount());

            // Include error details if the process failed
            if (ingestStatus.getPhase() == IngestStatus.IngestionPhase.FAILED) {
                statusMap.put("error", ingestStatus.getErrorMessage());
            }

            return ResponseEntity.ok(statusMap);
        } catch (Exception e) {
            // This is a fallback, but is unlikely to be hit as we are just reading state
            // from thread-safe singleton beans.
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to retrieve ingestion status.");
            errorResponse.put("details", e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * Retrieves the top email senders by message count.
     * @return A map containing the list of top senders.
     */
    @GetMapping("/top-senders")
    public ResponseEntity<Map<String, Object>> getTopSenders() {
        Map<String, Object> response = new HashMap<>();
        try {
            int TOP_SENDERS_LIMIT = 10;
            response.put("topSenders", ingestStatus.getTopSenders(TOP_SENDERS_LIMIT));
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("error", "Failed to get top senders");
            response.put("details", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
}