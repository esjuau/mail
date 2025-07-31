package group.mail.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class DatasetIngestionService {

    private static final String DATASET_URL = "https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz";
    
    private final DownloadService downloadService;
    private final ExtractionService extractionService;

    public DatasetIngestionService(DownloadService downloadService, ExtractionService extractionService) {
        this.downloadService = downloadService;
        this.extractionService = extractionService;
    }

    public CompletableFuture<Void> startIngestion() {
        log.info("Starting ingestion pipeline");
        
        return downloadService.downloadToTempAsync(DATASET_URL)
            .thenCompose(downloadedFile -> {
                log.info("Download completed: {}", downloadedFile);
                
                return extractionService.extractToTempDirectoryAsync(downloadedFile)
                    .whenComplete((extractedDir, ex) -> {
                        // Always clean up the downloaded file, regardless of extraction success/failure
                        cleanupFile(downloadedFile, "downloaded file");
                    });
            })
            .thenCompose(extractedDir -> {
                log.info("Extraction completed: {}", extractedDir);
                
                // Process the extracted directory here
                return processExtractedData(extractedDir)
                    .whenComplete((result, ex) -> {
                        // Always cleanup extracted directory after processing
                        cleanupDirectory(extractedDir, "extracted directory");
                    });
            })
            .handle((result, ex) -> {
                if (ex != null) {
                    log.error("Ingestion pipeline failed", ex);
                    throw new RuntimeException("Ingestion failed", ex);
                } else {
                    log.info("Ingestion pipeline completed successfully");
                    return result;
                }
            });
    }

    private CompletableFuture<Void> processExtractedData(Path extractedDir) {
        return CompletableFuture.runAsync(() -> {
            log.info("Processing extracted data from: {}", extractedDir);
            
            // TODO: Implement actual email processing logic here
            // - Parse email files
            // - Extract metadata 
            // - Store to database
            // - Generate statistics
            
            try {
                Thread.sleep(1000); // Simulate processing
                log.info("Data processing completed");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Processing interrupted", e);
            }
        });
    }

    private void cleanupFile(Path file, String description) {
        if (file != null) {
            try {
                boolean deleted = Files.deleteIfExists(file);
                if (deleted) {
                    log.debug("Cleaned up {}: {}", description, file);
                } else {
                    log.warn("Failed to delete {} (file not found): {}", description, file);
                }
            } catch (IOException e) {
                log.error("Failed to cleanup {}: {}", description, file, e);
            }
        }
    }

    private void cleanupDirectory(Path directory, String description) {
        if (directory != null) {
            try {
                // Use the same cleanup logic as ExtractionService
                Files.walk(directory)
                    .sorted((a, b) -> b.compareTo(a)) // Reverse order for proper directory deletion
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ex) {
                            log.warn("Failed to delete {}: {}", path, ex.getMessage());
                        }
                    });
                log.debug("Cleaned up {}: {}", description, directory);
            } catch (IOException e) {
                log.error("Failed to cleanup {}: {}", description, directory, e);
            }
        }
    }
}