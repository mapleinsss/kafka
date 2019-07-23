package org.maple.a_create.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @auther Mapleins
 * @date 2019-07-22 14:12
 * @Description
 */
public class Producer {

    private static final String BROKERLIST = "xodb-dev02:9092";
    private static final String TOPIC = "topic-ssl-test";

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.location", "G:/trust/server.truststore.jks");
        properties.put("ssl.truststore.password", "wisesoft");
        properties.put("ssl.truststore.type", "JKS");
        properties.put("ssl.keystore.location", "G:/server/server.keystore.jks");
        properties.put("ssl.keystore.password", "wisesoft");
        properties.put("ssl.keystore.type", "JKS");
        properties.put("ssl.keystore", "wisesoft");
        return properties;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello,Kafka!");
        // 异步发送
        producer.send(record).get();
        producer.close();
    }
}
