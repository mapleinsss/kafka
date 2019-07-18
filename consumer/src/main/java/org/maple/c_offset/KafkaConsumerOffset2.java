package org.maple.c_offset;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @auther Mapleins
 * @date 2019-06-25 17:36
 * @Description 模拟消息丢失
 */
@Slf4j
public class KafkaConsumerOffset2 {

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
            // 上一次提交的消费位移为 x+2k
            // 假设现在进行了一次 poll 操作，拉取到的消息为 x+2 -> x+8 那么就是拉取到了 [x+2,x+7] 的 6条数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            // 假设此时先进行了位移提交

            // 对数据进行操作...把消息保存到数据库里
            // 假设现在正在操作 x+5 这条数据，突然消费者宕机

            // 重启消费者，此时再去拉取消息，此时的消费位移变成了 x+8 ，也就是说 x+5 ~ x+7 的数据消费不到了，造成了消息的丢失。
        }

    }
}
