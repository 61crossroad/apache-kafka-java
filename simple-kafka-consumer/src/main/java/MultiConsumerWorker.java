import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class MultiConsumerWorker implements Runnable {

    private final Properties prop;
    private final String topic;
    private final String threadName;

    MultiConsumerWorker(Properties prop, String topic, int number) {
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
    }

    private void runWithSingleThread(String topic, Properties prop) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(List.of(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                log.info("{}", record);
            }
        }
    }

    private void runWithMultiThreads(String topic, Properties prop) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(List.of(topic));
        ExecutorService executorService = Executors.newCachedThreadPool();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            for (ConsumerRecord<String, String> record : records) {
                ConsumerWorker worker = new ConsumerWorker(record.value());
                executorService.execute(worker);
            }
        }
    }
}
