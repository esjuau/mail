package group.mail.services;

import group.mail.models.IngestStatus;
import group.mail.utils.FileUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class DatasetIngestionService {

    private static final String DATASET_URL = "https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz";

    private final DownloadService downloadService;
    private final ExtractionService extractionService;
    private final FileProcessor fileProcessor;
    private final IngestStatus status;

    /**
     * Initiates the entire data ingestion process.
     */

    public CompletableFuture<Void> startIngestion() {
        if (status.isRunning()) {
            String errorMessage = "Ingestion process is already running. Please wait for it to complete.";
            log.warn(errorMessage);
            throw new IllegalStateException(errorMessage);
        }

        initializeIngestionState();

        return runIngestionPipeline()
                .whenComplete(this::finalizeIngestionStatus);
    }

    /**
     * Initializes the state and metrics for the ingestion process.
     */
    private void initializeIngestionState() {
        log.info("Starting ingestion pipeline");
        status.start();
        fileProcessor.getIngestionMetricsData().reset();
    }

    /**
     * Defines and executes the entire asynchronous processing pipeline.
     * Stages: Download -> Extract & Process -> Cleanup.
     */
    private CompletableFuture<Void> runIngestionPipeline() {
        return downloadService.downloadToTempAsync(DATASET_URL)
                .thenCompose(this::extractProcessAndCleanup);
    }

    /**
     * Orchestrates the extraction and processing and cleans up the downloaded file.
     * This method ensures the downloaded file is deleted after the later
     * stages are complete (whether they succeed or fail).
     */
    private CompletableFuture<Void> extractProcessAndCleanup(Path downloadedFile) {
        return extractionService.extractToTempDirectoryAsync(downloadedFile)
                .thenCompose(this::processAndCleanupExtractedDirectory)
                .whenComplete((result, throwable) -> cleanupDownloadedFile(downloadedFile));
    }

    /**
     * Orchestrates the data processing and cleans up the extracted directory.
     * This method ensures the temporary extraction directory is deleted after processing.
     */
    private CompletableFuture<Void> processAndCleanupExtractedDirectory(Path extractedDir) {
        return processExtractedData(extractedDir)
                .whenComplete((result, throwable) -> cleanupExtractedDirectory(extractedDir));
    }

    /**
     * Initiates the actual data processing.
     */
    private CompletableFuture<Void> processExtractedData(Path extractedDir) {
        return CompletableFuture.runAsync(() -> {
            status.setPhase(IngestStatus.IngestionPhase.PROCESSING);
            fileProcessor.processRootDirectory(extractedDir);
        });
    }

    /**
     * Cleans up the extracted directory.
     */
    private void cleanupExtractedDirectory(Path extractedDir) {
        status.setPhase(IngestStatus.IngestionPhase.CLEANING_UP);
        log.info("Cleaning up extracted directory: {}", extractedDir);
        FileUtils.deleteRecursively(extractedDir);
    }

    /**
     * Cleans up the temporary downloaded file.
     */
    private void cleanupDownloadedFile(Path downloadedFile) {
        log.info("Cleaning up downloaded file: {}", downloadedFile);
        try {
            Files.deleteIfExists(downloadedFile);
        } catch (IOException e) {
            log.warn("Could not delete temporary downloaded file: {}", downloadedFile, e);
        }
    }

    /**
     * Updates the final status of the entire process (success/failure).
     */
    private void finalizeIngestionStatus(Void result, Throwable ex) {
        if (ex != null) {
            // Unwrap the actual cause from the CompletionException for clearer logging.
            Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
            log.error("Ingestion pipeline failed", cause);
            status.fail(cause);
        } else {
            log.info("Ingestion pipeline completed successfully");
            status.finish();
        }
    }
}