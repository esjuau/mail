package group.mail.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class IngestionController {

    @PostMapping("/start")
    public ResponseEntity<Map<String, String>> startIngestion() {
        Map<String, String> response = new HashMap<>();
        
        try {
            // TODO: Kutsu ingestion service metodia
            // ingestionService.startIngestion();
            
            response.put("message", "Ingestion started successfully");
            response.put("status", "started");
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            response.put("message", "Failed to start ingestion");
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getIngestionStatus() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            boolean isFinished = false;
            long totalMessages = 0L;
            
            response.put("finished", isFinished);
            response.put("totalMessagesProcessed", totalMessages);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            response.put("error", "Failed to get ingestion status");
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @GetMapping("/top-senders")
    public ResponseEntity<Map<String, Object>> getTopSenders() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            List<Object> topSenders = List.of();
            
            response.put("topSenders", topSenders);
            response.put("count", topSenders.size());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            response.put("error", "Failed to get top senders");
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
