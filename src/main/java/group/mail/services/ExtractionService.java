package group.mail.services;

import group.mail.models.IngestStatus;
import group.mail.utils.FileUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;

import static group.mail.models.IngestStatus.IngestionPhase.EXTRACTING;

/**
 * Service responsible for extracting a .tar.gz dataset file into a temp directory.
 */
@Slf4j
@Service
public class ExtractionService {

    private static final int BUFFER_SIZE = 64 * 1024;
    private final IngestStatus status;

    public ExtractionService(IngestStatus status) {
        this.status = status;
    }

    /**
     * Asynchronously extracts the given .tar.gz file into a uniquely named temp directory.
     * @param tarGzFile Path to the .tar.gz file
     * @return CompletableFuture that resolves to the path of the created extraction directory
     */
    @Async
    public CompletableFuture<Path> extractToTempDirectoryAsync(Path tarGzFile) {
        status.setPhase(EXTRACTING);
        return CompletableFuture.supplyAsync(() -> {
            try {
                return extractToTempDirectory(tarGzFile);
            } catch (IOException e) {
                log.error("Failed to extract tar.gz file", e);
                status.fail(e);
                throw new RuntimeException("Extraction failed", e);
            }
        });
    }

    /**
     * Extracts the given .tar.gz file into a uniquely named temp directory.
     * @param tarGzFile Path to the .tar.gz file
     * @return Path to the created extraction directory
     * @throws IOException on extraction or IO failure
     */
    public Path extractToTempDirectory(Path tarGzFile) throws IOException {
        Path extractDir = Files.createTempDirectory("dataset-extract-");
        log.info("Starting extraction of {} to directory {}", tarGzFile, extractDir);
        try (InputStream fis = Files.newInputStream(tarGzFile);
             BufferedInputStream bis = new BufferedInputStream(fis, BUFFER_SIZE);
             GZIPInputStream gzis = new GZIPInputStream(bis);
             TarArchiveInputStream tais = new TarArchiveInputStream(gzis)) {

            TarArchiveEntry entry;
            while ((entry = tais.getNextEntry()) != null) {
                Path targetPath = extractDir.resolve(entry.getName()).normalize();

                // Prevent directory traversal
                if (!targetPath.startsWith(extractDir)) {
                    throw new IOException("Entry is outside target dir: " + entry.getName());
                }

                if (entry.isDirectory()) {
                    Files.createDirectories(targetPath);
                } else {
                    Files.createDirectories(targetPath.getParent());
                    extractSingleFile(targetPath, tais);
                }
            }

            log.info("Extraction completed successfully to {}", extractDir);
            return extractDir;

        } catch (Exception e) {
            log.error("Extraction failed, cleaning up directory: {}", extractDir, e);
            status.fail(e);
            FileUtils.deleteRecursively(extractDir);
            throw new IOException("Failed to extract tar.gz file", e);
        } 
    }

    private void extractSingleFile(Path targetPath, TarArchiveInputStream tais) throws IOException {
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(targetPath.toFile()), BUFFER_SIZE)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = tais.read(buffer)) != -1) {
                bos.write(buffer, 0, len);
            }
        }
    }





}