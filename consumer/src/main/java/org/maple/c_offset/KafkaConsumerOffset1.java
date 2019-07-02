package org.maple.c_offset;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @auther Mapleins
 * @date 2019-06-25 17:36
 * @Description
 */
@Slf4j
public class KafkaConsumerOffset1 {

    private static final String BROKERLIST = "hadoop102:9092";
    private static final String TOPIC = "topic-demo";
    private static final String GOURPID = "group.demo";


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
        // 消费分区为 0
        TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
        // 订阅主题
        consumer.assign(Arrays.asList(topicPartition));
        // 当前消费到的位移
        long lastConsumedOffset = -1;
        // 循环消费消息
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.isEmpty()) {
                break;
            }
            List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
            lastConsumedOffset = records.get(records.size() - 1).offset();
            consumer.commitSync();// 同步提交消费位移
        }
        System.out.println("consumed offset is :" + lastConsumedOffset);
        OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
        System.out.println("committed offset is :" + offsetAndMetadata.offset());
        long position = consumer.position(topicPartition);
        System.out.println("the offset of the next record is " + position);
    }
}
