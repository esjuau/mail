package group.mail.services;

import group.mail.models.IngestStatus;
import group.mail.utils.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DatasetIngestionServiceCleanupTest {

    @Mock
    private DownloadService downloadService;
    @Mock
    private ExtractionService extractionService;
    @Mock
    private FileProcessor fileProcessor;
    @Mock
    private IngestStatus ingestStatus;

    private DatasetIngestionService ingestionService;
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("ingestion-test-");
        ingestionService = new DatasetIngestionService(
                downloadService, extractionService, fileProcessor, ingestStatus, "http://some.url"
        );
        when(ingestStatus.isRunning()).thenReturn(false);
    }

    @AfterEach
    void tearDown() {
        FileUtils.deleteRecursively(tempDir);
    }

    @Test
    void extractedDirectoryIsCleanedUp_whenProcessingFails() throws IOException {
        Path downloadedFile = tempDir.resolve("download.tar.gz");
        Path extractedDir = tempDir.resolve("extracted");
        Files.createDirectory(extractedDir);

        when(downloadService.downloadToTempAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(downloadedFile));
        when(extractionService.extractToTempDirectoryAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(extractedDir));
        doThrow(new RuntimeException("Simulated processing failure"))
                .when(fileProcessor).processRootDirectory(extractedDir);

        ingestionService.startIngestion();

        await().untilAsserted(() -> assertThat(extractedDir).doesNotExist());
    }

    @Test
    void downloadedFileIsDeleted_whenExtractionFails() throws IOException {
        Path downloadedFile = Files.createFile(tempDir.resolve("download.tar.gz"));
        assertThat(downloadedFile).exists();

        when(downloadService.downloadToTempAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(downloadedFile));
        when(extractionService.extractToTempDirectoryAsync(any()))
                .thenReturn(CompletableFuture.failedFuture(new IOException("Simulated extraction failure")));

        ingestionService.startIngestion();

        await().untilAsserted(() -> assertThat(downloadedFile).doesNotExist());
    }
}