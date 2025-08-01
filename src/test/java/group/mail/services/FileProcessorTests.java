package group.mail.services;

import group.mail.models.IngestStatus;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileProcessorTests {

    static class TestStatus extends IngestStatus {
        List<String> senders = new ArrayList<>();
        int processedCount = 0;

        @Override
        public void recordFile(String sender) {
            senders.add(sender);
        }

        @Override
        public void incrementProcessedFileCount() {
            processedCount++;
        }
    }

    @Test
    void processesSingleMailFileAndExtractsSender() throws Exception {
        Path dir = Files.createTempDirectory("maildir");
        Path mailFile = dir.resolve("enron.eml");
        String mail = """
                From: jeff.skilling@enron.com
                To: ken.lay@enron.com
                Subject: Test

                Hello Ken,
                """;
        Files.writeString(mailFile, mail);

        TestStatus status = new TestStatus();
        FileProcessor processor = new FileProcessor(status);

        processor.processRootDirectory(dir);

        assertThat(status.senders)
                .containsExactly("jeff.skilling@enron.com");
        assertThat(status.processedCount)
                .isZero();

        Files.deleteIfExists(mailFile);
        Files.deleteIfExists(dir);
    }
}
