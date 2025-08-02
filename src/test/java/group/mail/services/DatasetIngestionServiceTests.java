package group.mail.services;

import group.mail.models.IngestStatus;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DatasetIngestionServiceTests {

    @Test
    void startIngestion_runsPipelineAndCleansUp() {

        // init
        var downloadService = mock(DownloadService.class);
        var extractionService = mock(ExtractionService.class);
        var fileProcessor = mock(FileProcessor.class);
        var status = mock(IngestStatus.class);

        Path downloaded = Path.of("/tmp/test-download.tar.gz");
        Path extracted = Path.of("/tmp/extracted");

        when(status.isRunning()).thenReturn(false);

        when(downloadService.downloadToTempAsync(any()))
                .thenReturn(CompletableFuture.completedFuture(downloaded));

        when(extractionService.extractToTempDirectoryAsync(downloaded))
                .thenReturn(CompletableFuture.completedFuture(extracted));

        var service = new DatasetIngestionService(
                downloadService, extractionService, fileProcessor, status, "http://foo"
        );

        // start the pipeline
        service.startIngestion();

        await().atMost(5, SECONDS).untilAsserted(() -> verify(status).finish());

        // Check that the steps are run in the expected order
        InOrder inOrder = inOrder(status, downloadService, extractionService, fileProcessor);

        inOrder.verify(status).start();
        inOrder.verify(downloadService).downloadToTempAsync(any());
        inOrder.verify(extractionService).extractToTempDirectoryAsync(downloaded);
        inOrder.verify(status).setPhase(IngestStatus.IngestionPhase.PROCESSING);
        inOrder.verify(fileProcessor).processRootDirectory(extracted);
        inOrder.verify(status).setPhase(IngestStatus.IngestionPhase.CLEANING_UP);
        inOrder.verify(status).finish();
    }
}