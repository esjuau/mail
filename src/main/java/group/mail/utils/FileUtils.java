package group.mail.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static java.nio.file.FileVisitResult.CONTINUE;

@Slf4j
public class FileUtils {

    public static void deleteRecursively(Path path) {
        if (path == null || Files.notExists(path)) return;
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    try { Files.delete(file); }
                    catch (IOException e) { log.warn("failed to delete file {}: {}", file, e.toString()); }
                    return CONTINUE;
                }
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    if (exc != null) log.error("error visiting directory {}: {}", dir, exc.toString());
                    try { Files.delete(dir); }
                    catch (IOException e) { log.warn("failed to delete directory {}: {}", dir, e.toString()); }
                    return CONTINUE;
                }
            });
            log.debug("Cleaned up {}", path);
        } catch (IOException e) {
            log.error("Failed to cleanup {}", path, e);
        }
    }


}
