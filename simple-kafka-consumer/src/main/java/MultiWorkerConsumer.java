import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiWorkerConsumer {

    public static void runThreads(String TOPIC_NAME, Properties configs) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME));
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
