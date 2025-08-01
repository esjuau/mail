package group.mail.utils;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class EmailExtractorTest {

    @Test
    void extractSenderEmail_findsLightSenderInEnronFormat() throws Exception {
        String enronMail = """
                From: jeff.skilling@enron.com
                To: kenneth.lay@enron.com
                Subject: Test
                Date: Wed, 24 Jan 2001 10:00:00 -0800

                Hello Ken,
                This is a test message.
                """;
        Path temp = Files.createTempFile("mail", ".eml");
        Files.writeString(temp, enronMail);

        try {
            Optional<String> sender = EmailExtractor.extractSenderEmail(temp);
            assertThat(sender)
                    .isPresent()
                    .contains("jeff.skilling@enron.com");
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    @Test
    void extractSenderEmail_fallbackParsesSenderWhenNoLightFrom() throws Exception {
        String mail = """
            To: kenneth.lay@enron.com
            Subject: Test
            Date: Wed, 24 Jan 2001 10:00:00 -0800
            MIME-Version: 1.0
            Content-Type: text/plain; charset=UTF-8
            From: jeff.skilling@enron.com

            Hello Ken,
            """;
        Path temp = Files.createTempFile("mail", ".eml");
        Files.writeString(temp, mail);

        try {
            Optional<String> sender = EmailExtractor.extractSenderEmail(temp);
            assertThat(sender)
                    .isPresent()
                    .contains("jeff.skilling@enron.com");
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    @Test
    void extractSenderEmail_returnsEmptyWhenMailIsBroken() throws Exception {
        String mail = """
            To: kenneth.lay@enron.com
            Subject: Test

            Hello Ken,
            """;
        Path temp = Files.createTempFile("mail", ".eml");
        Files.writeString(temp, mail);

        try {
            Optional<String> sender = EmailExtractor.extractSenderEmail(temp);
            assertThat(sender).isEmpty();
        } finally {
            Files.deleteIfExists(temp);
        }
    }

}
