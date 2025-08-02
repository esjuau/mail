package group.mail.services;

import group.mail.models.IngestStatus;
import group.mail.utils.EmailExtractor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@Slf4j
@Component
@RequiredArgsConstructor
public class FileProcessor {

    @Value("${sequential.threshold:10}")
    private int SEQUENTIAL_THRESHOLD;
    private final IngestStatus status;

    public void processRootDirectory(Path rootDir) {
        log.info("Starting processing job for root: {}. Status tracking started.", rootDir);
        try {
            processPath(rootDir);
        } catch (Exception e) {
            status.fail(e);
            log.error("Processing failed for root {}: {}", rootDir, e.getMessage(), e);
        } finally {
            log.info("Processing job finished for root");
        }
    }

    private void processPath(Path path) throws IOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new IOException("Thread was interrupted");
        }

        if (Files.isRegularFile(path)) {
            processSingleFile(path);
        } else if (Files.isDirectory(path)) {
            processDirectory(path);
        }
    }

    private void processDirectory(Path dir) throws IOException {
        List<Path> children;
        try (Stream<Path> fileStream = Files.list(dir)) {
            children = fileStream.toList();
        } catch (IOException e) {
            log.warn("Failed to list directory {}: {}", dir, e.getMessage());
            return;
        }

        if (children.isEmpty()) {
            return;
        }

        if (children.size() <= SEQUENTIAL_THRESHOLD) {
            for (Path child : children) {
                processPath(child);
            }
        } else {
            processDirectoryConcurrently(children);
        }
    }

    /**
     * Manages concurrent processing of a list of paths using CompletableFuture
     * with a virtual-thread-per-task executor. Futures are handled within this method.
     */
    private void processDirectoryConcurrently(List<Path> paths) {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            int mid = paths.size() / 2;
            List<Path> firstHalf = paths.subList(0, mid);
            List<Path> secondHalf = paths.subList(mid, paths.size());

            CompletableFuture<Void> future1 = createChunkProcessingFuture(firstHalf, executor);
            CompletableFuture<Void> future2 = createChunkProcessingFuture(secondHalf, executor);

            CompletableFuture.allOf(future1, future2).join();

        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    /**
     * Creates a CompletableFuture that processes a "chunk" (list of paths)
     * on the given ExecutorService.
     */
    private CompletableFuture<Void> createChunkProcessingFuture(List<Path> chunk, ExecutorService executor) {
        return CompletableFuture.runAsync(() -> {
            try {
                for (Path path : chunk) {
                    processPath(path);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, executor);
    }

    /**
     * Processes a single file to extract sender information.
     */
    private void processSingleFile(Path path) {
        try {
            Optional<String> from = EmailExtractor.extractSenderEmail(path);
            from.ifPresentOrElse(
                    status::recordFile,
                    status::incrementProcessedFileCount
            );
        } catch (Exception e) {
            log.warn("Failed to parse file {}: {}", path, e.getMessage());
            status.incrementProcessedFileCount(); // Count it even if it fails to parse
        }
    }
}