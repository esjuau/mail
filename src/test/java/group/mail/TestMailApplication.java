package group.mail;

import org.springframework.boot.SpringApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
public class TestMailApplication {

    public static void main(String[] args) {
        SpringApplication.from(MailApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
