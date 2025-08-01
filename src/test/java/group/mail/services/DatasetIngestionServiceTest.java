package group.mail.services;

import group.mail.models.IngestStatus;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DatasetIngestionServiceTest {

    @Test
    @SneakyThrows
    void startIngestion_runsPipelineAndCleansUp() {

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

        service.startIngestion();

        Thread.sleep(200);

        // check that the below methods get called
        verify(downloadService).downloadToTempAsync(any());
        verify(extractionService).extractToTempDirectoryAsync(downloaded);
        verify(fileProcessor).processRootDirectory(extracted);

        verify(status).start();
        verify(status).setPhase(IngestStatus.IngestionPhase.PROCESSING);
        verify(status).setPhase(IngestStatus.IngestionPhase.CLEANING_UP);
        verify(status).finish();

    }
}
