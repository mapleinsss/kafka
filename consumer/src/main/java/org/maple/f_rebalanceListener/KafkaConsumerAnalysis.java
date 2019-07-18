package org.maple.f_rebalanceListener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @auther Mapleins
 * @date 2019-06-25 17:36
 * @Description 再均衡
 */
@Slf4j
public class KafkaConsumerAnalysis {

    private static final String BROKERLIST = "hadoop102:9092";
    private static final String TOPIC = "topic-demo";
    private static final String GOURPID = "group.demo";
    private static final AtomicBoolean ISRUNNING = new AtomicBoolean(true);


    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GOURPID);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        // 创建一个消费客户端实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        // 订阅主题
        consumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // do nothing
            }
        });

        // 循环消费消息
        try {
            while (ISRUNNING.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(x -> System.out.println("topic:" + x.topic() + "\t partition:" + x.partition() +
                        "\t offset:" + x.offset() + "\t key:" + x.key() + "\t value:" + x.value()));

                for (ConsumerRecord<String, String> record : records) {
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                }
                consumer.commitAsync(currentOffsets,null);
            }
        } catch (Exception e) {
            log.error("occur exception ", e);
        } finally {
            consumer.close();
        }
    }
}
