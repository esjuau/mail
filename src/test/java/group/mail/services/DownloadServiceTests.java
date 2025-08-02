package group.mail.services;

import group.mail.models.IngestStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.io.ResourceLoader;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SpringBootTest
class DownloadServiceTests {

    @Mock
    private RestTemplateBuilder restTemplateBuilder;
    @Mock
    private RestTemplate restTemplate;
    @Mock
    private ResourceLoader resourceLoader;
    @Mock
    private IngestStatus status;

    @Test
    void downloadToTempAsync_deletesRealFile_whenDownloadFails() throws IOException {

        when(restTemplateBuilder.connectTimeout(any(Duration.class))).thenReturn(restTemplateBuilder);
        when(restTemplateBuilder.build()).thenReturn(restTemplate);

        DownloadService service = new DownloadService(restTemplateBuilder, 1, "", resourceLoader, status);

        // simulate network error
        when(restTemplate.execute(anyString(), any(), any(), any()))
                .thenThrow(new RuntimeException("Simulated network failure"));

        // create a real file to follow
        Path knownTempFile = Files.createTempFile("test-download-", ".tmp");
        assertThat(knownTempFile).exists();

        // modify Files.createTempFile to return our test file
        try (MockedStatic<Files> mockedFiles = mockStatic(Files.class,
                withSettings().defaultAnswer(CALLS_REAL_METHODS))) {

            // return our temp file
            mockedFiles.when(() -> Files.createTempFile(anyString(), anyString())).thenReturn(knownTempFile);

            assertThatThrownBy(() -> service.downloadToTempAsync("http://some.url"))
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(IOException.class);

            // check that the cleanup worked and the file is deleted
            assertThat(knownTempFile).doesNotExist();
        }
    }
}