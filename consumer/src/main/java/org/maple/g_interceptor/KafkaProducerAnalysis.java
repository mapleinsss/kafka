package org.maple.g_interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @auther Mapleins
 * @date 2019-06-23 18:33
 * @Description 改进的客户端：config 配置可以参考 ProducerConfig ，全类名字可以通过 Java Api
 */
public class KafkaProducerAnalysis {

    private static final String BROKERLIST = "hadoop102:9092";
    private static final String TOPIC = "topic-a";

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return properties;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record1 = new ProducerRecord<>(TOPIC, 0, 10L, null, "first-expire-data");
        ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC, 0, 8L, null, "normal-data");
        ProducerRecord<String, String> record3 = new ProducerRecord<>(TOPIC, 0, 11L, null, "last-expire-data");
        producer.send(record1).get();
        producer.send(record2).get();
        producer.send(record3).get();
        producer.close();
    }

}
