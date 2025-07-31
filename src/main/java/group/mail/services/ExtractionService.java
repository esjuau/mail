package group.mail.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;

/**
 * Service responsible for extracting a .tar.gz dataset file into a temp directory.
 */
@Slf4j
@Service
public class ExtractionService {

    private static final int BUFFER_SIZE = 64 * 1024;

    /**
     * Asynchronously extracts the given .tar.gz file into a uniquely named temp directory.
     * @param tarGzFile Path to the .tar.gz file
     * @return CompletableFuture that resolves to the path of the created extraction directory
     */
    @Async
    public CompletableFuture<Path> extractToTempDirectoryAsync(Path tarGzFile) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return extractToTempDirectory(tarGzFile);
            } catch (IOException e) {
                log.error("Failed to extract tar.gz file", e);
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
            cleanupDirectory(extractDir);
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

    public void cleanupDirectory(Path directory) {
        if (directory == null || Files.notExists(directory)) return;
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    try {
                        Files.deleteIfExists(file);
                    } catch (IOException e) {
                        log.warn("failed to delete file {}: {}", file, e.toString());
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    if (exc != null) {
                        log.error("error visiting directory {}: {}", dir, exc.toString());
                    }
                    try {
                        Files.deleteIfExists(dir);
                    } catch (IOException e) {
                        log.warn("failed to delete directory {}: {}", dir, e.toString());
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
            log.debug("Cleaned up directory {}", directory);
        } catch (IOException e) {
            log.error("Failed to cleanup directory {}", directory, e);
        }
    }



}