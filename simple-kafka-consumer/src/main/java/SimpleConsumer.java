import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
public class SimpleConsumer {

    private final static String TOPIC_NAME = "test";
    private final static int PARTITION_NUMBER = 0;
    private final static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        Properties adminConfigs = new Properties();
        adminConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        AdminClient admin = AdminClient.create(adminConfigs);

        log.info("== Get broker information");
        for (Node node : admin.describeCluster().nodes().get()) {
            log.info("node: {}", node);
            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
            describeConfigs.all().get().forEach((broker, config) ->
                config.entries().forEach(configEntry -> log.info(configEntry.name() + "= " + configEntry.value())));
        }

        log.info("== AllTopicNames");
        Map<String, TopicDescription> topicInformation = admin.describeTopics(Collections.singletonList(TOPIC_NAME)).allTopicNames().get();
        log.info("{}", topicInformation);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
//        consumer.subscribe(List.of(TOPIC_NAME));

        RebalanceListener rebalanceListener = new RebalanceListener(consumer);
        consumer.subscribe(List.of(TOPIC_NAME), rebalanceListener);

//        consumer.assign(Collections.singletonList(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));
        Set<TopicPartition> assignedTopicPartition = consumer.assignment();
        assignedTopicPartition.forEach(tp -> log.info(tp.toString()));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
//            Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    log.info("{}", record);
//                    currentOffset.put(
//                            new TopicPartition(record.topic(), record.partition()),
//                            new OffsetAndMetadata(record.offset() + 1, null));
//                    consumer.commitSync(currentOffset);
                    
                    rebalanceListener.addOffsetToTrack(record.topic(), record.partition(), record.offset());
                }

                consumer.commitAsync();

//            consumer.commitAsync((offsets, exception) -> {
//                if (exception != null) {
//                    log.error("Commit failed for offsets {}", offsets, exception);
//                } else {
//                    log.info("Commit succeed");
//                }
//            });
            }
        } catch (WakeupException e) {
            log.warn("Wakeup consumer");
        } finally {
            consumer.close();
        }
    }

    @RequiredArgsConstructor
    static class RebalanceListener implements ConsumerRebalanceListener {

        private final KafkaConsumer<String, String> consumer;
        private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.warn("Partitions are assigned");
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.warn("Partitions are revoked");
            consumer.commitSync(currentOffsets);

            currentOffsets.clear();
        }

        public void addOffsetToTrack(String topic, int partition, long offset) {
            currentOffsets.put(
                    new TopicPartition(topic, partition),
                    new OffsetAndMetadata(offset + 1, null));
        }

        public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
            return currentOffsets;
        }
    }
}
