package org.maple.d_commit.sync;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * @auther Mapleins
 * @date 2019-07-07 12:48
 * @Description 同步提交指定位移进行提交 伪代码
 */
public class SyncCommit4 {

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
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 手动提交
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (TopicPartition partition : records.partitions()) { // 遍历所有分区
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition); // 拿到一个分区的消息
                    for (ConsumerRecord<String, String> record : partitionRecords) { // 遍历该分区消息
                        // do some logical process
                    }
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset(); // 拿到最后消费的位移
                    kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastConsumedOffset + 1)));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }

}
