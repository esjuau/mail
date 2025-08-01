package group.mail.utils;

import org.apache.james.mime4j.dom.Message;
import org.apache.james.mime4j.dom.address.Mailbox;
import org.apache.james.mime4j.dom.address.MailboxList;
import org.apache.james.mime4j.message.DefaultMessageBuilder;
import org.apache.james.mime4j.stream.MimeConfig;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

@Component
public final class EmailExtractor {

    private static final MimeConfig MIME_CONFIG = MimeConfig.custom()
            .setMaxLineLen(0)          // unlimited
            .setMaxHeaderLen(200_000)
            .build();

    public EmailExtractor() {}

    public static Optional<String> extractSenderEmail(Path path) {
        // try light way first
        try (BufferedReader br = Files.newBufferedReader(path)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) break;
                if (line.regionMatches(true, 0, "From:", 0, 5)) {
                    String candidate = line.substring(5).trim();
                    if (!candidate.isBlank()) {
                        return Optional.of(candidate);
                    }
                }
            }
        } catch (IOException ignored) {
        }

        // fallback
        try (var in = Files.newInputStream(path)) {
            DefaultMessageBuilder builder = new DefaultMessageBuilder();
            builder.setMimeEntityConfig(MIME_CONFIG);
            Message message = builder.parseMessage(in);
            MailboxList from = message.getFrom();
            if (from != null && !from.isEmpty()) {
                Mailbox mailbox = from.getFirst();
                if (mailbox != null) {
                    return Optional.ofNullable(mailbox.getAddress());
                }
            }
        } catch (IOException ignored) {
        }
        return Optional.empty();
    }
}
