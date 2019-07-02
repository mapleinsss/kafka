package org.maple.b_consumeMessage;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @auther Mapleins
 * @date 2019-06-25 17:36
 * @Description 消费指定主题-分区的数据
 */
@Slf4j
public class KafkaConsumerTopic_Partition {

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

        // 循环消费消息
        try {
            while (ISRUNNING.get()) {
                /**
                 * 获取消息集中指定分区信息
                 */
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                Set<TopicPartition> partitions = records.partitions(); // 获取所有的主题-分区
                for (TopicPartition topicPartition : partitions) {
                    // 通过 ConsumerRecords.records(TopicPartition arg) 传递主题-分区，就可以获取指定主题分区的 record。
                    List<ConsumerRecord<String, String>> consumerRecordList = records.records(topicPartition);
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecordList) {
                        System.out.println(consumerRecord.partition()+"\t"+consumerRecord.value());
                    }
                }
            }
        } catch (Exception e) {
            log.error("occur exception ", e);
        } finally {
            consumer.close();
        }
    }
}
