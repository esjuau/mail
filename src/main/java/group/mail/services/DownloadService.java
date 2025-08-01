package group.mail.services;

import group.mail.models.IngestStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static group.mail.models.IngestStatus.IngestionPhase.DOWNLOADING;

/**
 * Service responsible for downloading a .tar.gz dataset from a given URL into a temp file,
 * with progress tracking.
 */
@Slf4j
@Service
public class DownloadService {

    private static final int BUFFER_SIZE = 64 * 1024; // 64 KB

    private final RestTemplate restTemplate;
    private final IngestStatus status;

    public DownloadService(RestTemplateBuilder builder,
                           @Value("${file.download.timeout.hours:2}") int timeoutHours,
                           IngestStatus status) {
        this.status = status;
        this.restTemplate = builder
                .connectTimeout(Duration.ofHours(timeoutHours))
                .build();
    }

    @Async
    public CompletableFuture<Path> downloadToTempAsync(String url) {
        try {
            status.setPhase(DOWNLOADING);
            return CompletableFuture.completedFuture(downloadToTemp(url));
        } catch (IOException e) {
            status.fail(e);
            log.error("Failed to download file from {}", url, e);
            throw new CompletionException(e);
        }
    }

    /**
     * Downloads the file at the given URL into a uniquely named temp file.
     *
     * @param url dataset URL (must be http or https)
     * @return Path to the downloaded temp file
     * @throws IOException on download or IO failure
     */
    private Path downloadToTemp(String url) throws IOException {

        Path tempFile = Files.createTempFile("dataset-download-", ".tar.gz");
        log.info("Starting download from {} to {}", url, tempFile);

        try {
            restTemplate.execute(
                    url,
                    HttpMethod.GET,
                    request -> {
                        request.getHeaders().add("Accept-Encoding", "identity");
                        request.getHeaders().add("User-Agent", "Dataset-Ingestion/1.0");
                    },
                    (ClientHttpResponse response) -> {
                        long contentLength = response.getHeaders().getContentLength();
                        if (contentLength > 0) {
                            log.info("Total bytes to download: {}", contentLength);
                        }
                        String contentType = response.getHeaders().getContentType() != null
                                ? response.getHeaders().getContentType().toString() : "";
                        if (!contentType.contains("gzip") && !contentType.contains("octet-stream")) {
                            log.warn("Unexpected content type: {}", contentType);
                        }

                        try (InputStream is = response.getBody();
                             BufferedInputStream bis = new BufferedInputStream(is, BUFFER_SIZE);
                             BufferedOutputStream bos = new BufferedOutputStream(
                                     new FileOutputStream(tempFile.toFile()), BUFFER_SIZE)) {

                            byte[] buffer = new byte[BUFFER_SIZE];
                            int bytesRead;

                            while ((bytesRead = bis.read(buffer)) != -1) {
                                bos.write(buffer, 0, bytesRead);
                            }
                        }
                        return null;
                    }
            );

            log.info("Download completed successfully: {}", tempFile);
            return tempFile;

        } catch (Exception e) {
            log.error("Download failed, cleaning up temp file: {}", tempFile, e);
            Files.deleteIfExists(tempFile);
            throw e;
        }
    }


}
