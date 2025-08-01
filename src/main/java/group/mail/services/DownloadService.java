package group.mail.services;

import group.mail.models.IngestStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpMethod;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static group.mail.models.IngestStatus.IngestionPhase.DOWNLOADING;

/**
 * Service responsible for downloading a .tar.gz dataset from a given URL or
 * copying it from a local classpath resource into a temp file.
 */
@Slf4j
@Service
public class DownloadService {

    private static final int BUFFER_SIZE = 64 * 1024; // 64 KB

    private final RestTemplate restTemplate;
    private final IngestStatus status;
    private final String localDatasetPath;
    private final ResourceLoader resourceLoader;

    public DownloadService(RestTemplateBuilder builder,
                           @Value("${file.download.timeout.hours:2}") int timeoutHours,
                           @Value("${dataset.local.path:}") String localDatasetPath,
                           ResourceLoader resourceLoader,
                           IngestStatus status) {
        this.status = status;
        this.localDatasetPath = localDatasetPath;
        this.resourceLoader = resourceLoader;
        this.restTemplate = builder
                .connectTimeout(Duration.ofHours(timeoutHours))
                .build();

    }

    /**
     * Asynchronously "downloads" the dataset.
     * It will either copy a local file or download from a URL, based on configuration.
     *
     * @param url The dataset URL (used only if a local path is not configured).
     * @return A CompletableFuture containing the path to the temporary file.
     */
    @Async
    public CompletableFuture<Path> downloadToTempAsync(String url) {
        try {
            status.setPhase(DOWNLOADING);
            return CompletableFuture.completedFuture(downloadToTemp(url));
        } catch (IOException e) {
            status.fail(e);
            log.error("Failed to prepare dataset file from URL {} or local path {}", url, localDatasetPath, e);
            throw new CompletionException(e);
        }
    }

    /**
     * Prepares a temporary file containing the dataset.
     * It delegates to either the local copy or URL download method.
     *
     * @param url The remote URL (fallback).
     * @return Path to the temp file.
     * @throws IOException If file access fails.
     */
    private Path downloadToTemp(String url) throws IOException {
        if (localDatasetPath != null && !localDatasetPath.isBlank()) {
            return copyFromLocalResource();
        } else {
            return downloadFromUrl(url);
        }
    }

    /**
     * Copies a dataset from a classpath resource to a temp file.
     *
     * @return Path to the created temp file.
     * @throws IOException if the resource is not found or cannot be copied.
     */
    private Path copyFromLocalResource() throws IOException {
        log.info("Attempting to load local resource from path: {}", localDatasetPath);
        Resource resource = resourceLoader.getResource(localDatasetPath);
        if (!resource.exists()) {
            throw new IOException("Local dataset resource not found at: " + localDatasetPath);
        }

        Path tempFile = Files.createTempFile("dataset-local-", ".tar.gz");
        log.info("Copying local dataset to {}", tempFile);

        try (InputStream is = resource.getInputStream()) {
            Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            log.error("Failed to copy local dataset, cleaning up temp file: {}", tempFile, e);
            Files.deleteIfExists(tempFile);
            throw e;
        }

        log.info("Local dataset copied successfully: {}", tempFile);
        return tempFile;
    }

    /**
     * Downloads the file at the given URL into a uniquely named temp file.
     *
     * @param url dataset URL (must be http or https)
     * @return Path to the downloaded temp file
     * @throws IOException on download or IO failure
     */
    private Path downloadFromUrl(String url) throws IOException {
        Path tempFile = Files.createTempFile("dataset-download-", ".tar.gz");
        log.info("Starting download from {} to {}", url, tempFile);

        try {
            restTemplate.execute(url, HttpMethod.GET, null, response -> {
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
            });
            log.info("Download completed successfully: {}", tempFile);
            return tempFile;
        } catch (Exception e) {
            log.error("Download failed, cleaning up temp file: {}", tempFile, e);
            Files.deleteIfExists(tempFile);
            throw new IOException("Failed to download file from " + url, e);
        }
    }
}