package org.maple.e_seek;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @auther Mapleins
 * @date 2019-06-25 17:36
 * @Description offsetsForTimes() 从指定时间开始消费
 */
@Slf4j
public class KafkaConsumerAnalysis4 {

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
        // 订阅主题
        consumer.subscribe(Collections.singletonList(TOPIC));

        // 获取所有的主题
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) { // 设置较短的获取时间，来确保不会耽误太多时间在获取分区主题上
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        // 存放时间戳和主题分区
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
            timestampsToSearch.put(topicPartition, System.currentTimeMillis() - 24 * 3600 * 1000); // 一天前
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampsToSearch); // 获取前一天所有分区的消费位移
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(topicPartition); // 获取该分区的消费位移
            if (offsetAndTimestamp != null) {
                consumer.seek(topicPartition,offsetAndTimestamp.offset()); // 从前一天的这个时间开始消费
            }
        }


        // 循环消费消息
        try {
            while (ISRUNNING.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(x -> System.out.println("topic:" + x.topic() + "\t partition:" + x.partition() +
                        "\t offset:" + x.offset() + "\t key:" + x.key() + "\t value:" + x.value()));
            }
        } catch (Exception e) {
            log.error("occur exception ", e);
        } finally {
            consumer.close();
        }
    }
}
