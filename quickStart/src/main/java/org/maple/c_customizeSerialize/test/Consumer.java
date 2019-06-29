package org.maple.c_customizeSerialize.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.maple.c_customizeSerialize.Company;
import org.maple.c_customizeSerialize.CompanyDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @auther Mapleins
 * @date 2019-06-25 17:35
 * @Description
 */
public class Consumer {

    private static final String BROKERLIST = "hadoop102:9092";
    private static final String TOPIC = "topic-demo";
    private static final String GOURPID = "group.id";


    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GOURPID);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        // 创建一个消费客户端实例
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<>(properties);
        // 订阅主题
        consumer.subscribe(Collections.singletonList(TOPIC));
        // 循环消费消息
        while (true) {
            ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(x -> System.out.println(x.value()));
        }
    }
}
