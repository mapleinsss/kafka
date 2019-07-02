package org.maple.a_consumer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @auther Mapleins
 * @date 2019-06-23 18:33
 * @Description 改进的客户端：config 配置可以参考 ProducerConfig ，全类名字可以通过 Java Api
 */
public class KafkaProducerAnalysis {

    private static final String BROKERLIST = "hadoop102:9092";
    private static final String TOPIC = "topic-demo";

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello,Kafka!");
        // 异步发送
        producer.send(record, (metadata, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                System.out.println(metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset());
            }
        });
        producer.close();
    }

}
