package org.maple.g_interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @auther Mapleins
 * @date 2019-07-11 9:39
 * @Description Time to Live 过期时间,过滤掉 消息自带的时间戳属性 > 过期时间
 */
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String, String> {

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        long now = System.currentTimeMillis();
        // 存放拦截处理后的消息
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        for (TopicPartition partition : records.partitions()) { // 遍历主题分区
            List<ConsumerRecord<String, String>> tpRecords = records.records(partition); // 该主题分区所有记录 records
            // 存放新的满足小于过期时间的 record
            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : tpRecords) {
                if (record.timestamp() < 10L) {
                    newTpRecords.add(record);
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(partition, newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void close() {

    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((x, y) -> System.out.println("Interceptor: ->>>> TopicPartition:" + x + "\toffsets:" + y.offset()));
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
