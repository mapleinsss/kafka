package org.maple.b_configAndSendType;

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
        // 发后即忘，异步
//        try {
//            producer.send(record);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        // 同步发送1
//        try {
//            producer.send(record).get();
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//        }
        // 同步发送2
//        try {
//            Future<RecordMetadata> future = producer.send(record);
//            RecordMetadata metadata = future.get();
//            System.out.println(metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset());
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//        }
        // 异步发送
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println(metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset());
                }
            }
        });
        producer.close();
    }

}
