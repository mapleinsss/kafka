package org.maple.e_customizeIntercepter;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @auther Mapleins
 * @date 2019-06-27 17:16
 * @Description 生产者拦截器
 */
public class CustomizeInterceptor implements ProducerInterceptor<String, String> {

    private AtomicLong success = new AtomicLong(0);
    private AtomicLong failure = new AtomicLong(0);

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String value = record.value();
        // 修改 value 的值
        String modifiedValue = "value have change by intercepter -->" + value;
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), modifiedValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            success.getAndIncrement();
        } else {
            failure.getAndIncrement();
        }
    }

    @Override
    public void close() {
        long total = success.get() + failure.get();
        double successRatio = success.get() / total;
        System.out.println("[info] 发送成功的概率为 --> " + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
