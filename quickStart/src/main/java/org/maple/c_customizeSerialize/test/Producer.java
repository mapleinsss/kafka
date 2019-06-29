package org.maple.c_customizeSerialize.test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.maple.c_customizeSerialize.Company;
import org.maple.c_customizeSerialize.CompanySerializer;

import java.util.Properties;

/**
 * @auther Mapleins
 * @date 2019-06-25 17:27
 * @Description
 */
public class Producer {
    private static final String BROKERLIST = "hadoop102:9092";
    private static final String TOPIC = "topic-demo";

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);
        Company company = Company.builder().name("maple.org").address("To the moon").build();
        ProducerRecord<String,Company> record = new ProducerRecord<>(TOPIC, company);
        try {
            producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}
