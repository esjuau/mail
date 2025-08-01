package group.mail.services;

import group.mail.models.IngestStatus;
import group.mail.utils.EmailExtractor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.StructuredTaskScope;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class FileProcessor {

    private static final int THRESHOLD = 50;
    private final IngestStatus status;

    public FileProcessor(IngestStatus status) {
        this.status = status;
    }

    public void processRootDirectory(Path rootDir) {
        log.info("Starting processing job for root: {}. Status tracking started.", rootDir);
        try {
            try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                scope.fork(() -> processPath(rootDir));
                scope.join();
                scope.throwIfFailed();
            }
        } catch (Exception e) {
            status.fail(e);
            log.error("Processing failed for root {}: {}", rootDir, e.getMessage());
        } finally {
            log.info("Processing job finished for root");
        }
    }

    private Void processPath(Path path) throws Exception {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException();

        if (Files.isRegularFile(path)) {
            processSingleFile(path);
        } else if (Files.isDirectory(path)) {
            List<Path> children;
            try (Stream<Path> fileStream = Files.list(path)) {
                children = fileStream.collect(Collectors.toList());
            } catch (IOException e) {
                log.warn("Failed to list directory {}: {}", path, e.getMessage());
                return null;
            }

            if (children.size() <= THRESHOLD) {
                for (Path child : children) {
                    if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
                    processPath(child);
                }
            } else {
                int mid = children.size() / 2;
                List<Path> first = children.subList(0, mid);
                List<Path> second = children.subList(mid, children.size());

                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    scope.fork(() -> processChunk(first));
                    scope.fork(() -> processChunk(second));
                    scope.join();
                    scope.throwIfFailed();
                }
            }
        }
        return null;
    }

    private Void processChunk(List<Path> chunk) throws Exception {
        for (Path p : chunk) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            processPath(p);
        }
        return null;
    }

    private void processSingleFile(Path path) {
        Optional<String> from = EmailExtractor.extractSenderEmail(path);
        from.ifPresentOrElse(status::recordFile,
                status::incrementProcessedFileCount);
    }
}