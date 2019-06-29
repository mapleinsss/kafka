package org.maple.a_quickstart;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @auther Mapleins
 * @date 2019-06-23 13:29
 * @Description 生产者
 */
public class ProducerQuickStart {

    private static final String BROKERLIST = "hadoop102:9092";
    private static final String TOPIC = "topic-demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer" );
        properties.put("bootstrap.servers",BROKERLIST);
        // 配置生产者客户端参数并创建 KafkaProducer 实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 构建所需要发送的消息
        ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC,"hello,Kafka!");
        // 发送消息
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 关闭生产者客户端示例
        producer.close();
    }

}
