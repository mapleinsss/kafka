package org.maple.c_offset;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @auther Mapleins
 * @date 2019-06-25 17:36
 * @Description 模拟重复消费
 */
@Slf4j
public class KafkaConsumerOffset3 {

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
        // 循环消费消息
        while (true) {
            // 现在已经拉取到所有消息
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            // 执行操作中...

            // 进行位移提交

            /**
             * 如果在上面执行操作时，客户端突然宕机，未进行位移提交，那么当故障恢复后，又要从上次位移提交的位置开始消费，造成了重复消费。
             */
        }

    }
}
