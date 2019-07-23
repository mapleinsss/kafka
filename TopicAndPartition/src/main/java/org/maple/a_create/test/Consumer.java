package org.maple.a_create.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @auther Mapleins
 * @date 2019-07-22 14:12
 * @Description
 */
@Slf4j
public class Consumer {

    private static final String BROKERLIST = "hadoop102:9093";
    private static final String TOPIC = "topic-ssl-1";
    private static final String GOURPID = "group.demo";
    private static final AtomicBoolean ISRUNNING = new AtomicBoolean(true);


    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GOURPID);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.put("security.protocol", "SSL");
//        properties.put("ssl.truststore.location", "G:/trust/server.truststore.jks");
//        properties.put("ssl.truststore.password", "wisesoft");
//        properties.put("ssl.truststore.type", "JKS");
//        properties.put("ssl.keystore.location", "G:/server/server.keystore.jks");
//        properties.put("ssl.keystore.password", "wisesoft");
//        properties.put("ssl.keystore.type", "JKS");
//        properties.put("ssl.keystore", "wisesoft");
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
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(x -> System.out.println("topic:" + x.topic() + "\t partition:" + x.partition() +
                        "\t offset:" + x.offset() + "\t key:" + x.key() + "\t value:" + x.value()));
            }
        } catch (Exception e) {
            log.error("occur exception ", e);
        } finally {
            consumer.close();
        }
    }
}
