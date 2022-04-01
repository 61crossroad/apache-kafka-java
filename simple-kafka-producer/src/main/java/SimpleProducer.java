import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class SimpleProducer {

    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String now = LocalDateTime.now(ZoneId.of("Asia/Seoul")).toString();
        String messageValue = "testMessage " + now;
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        RecordMetadata metadata = producer.send(record).get();
        log.info(metadata.toString());

        ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC_NAME, "Seoul", "16 " + now);
        producer.send(record2, new ProducerCallback());

        int partitionNo = 0;
        ProducerRecord<String, String> record3 = new ProducerRecord<>(
                TOPIC_NAME, partitionNo, "Pangyo", "23 " + now);
        producer.send(record3);

        producer.flush();
        producer.close();
    }
}
