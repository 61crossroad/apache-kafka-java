package com.ohzae.springkafkaproducer;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class ProducerRunner implements CommandLineRunner {

    private final KafkaTemplate<Integer, String> template;

    @Override
    public void run(String... args) {
        String TOPIC_NAME = "test";

        for (int i = 0; i < 10; i++) {
            template.send(TOPIC_NAME, "test" + i);
        }
        System.exit(0);
    }
}
