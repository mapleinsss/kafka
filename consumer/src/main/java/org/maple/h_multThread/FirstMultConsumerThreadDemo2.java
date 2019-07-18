package org.maple.h_multThread;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @auther Mapleins
 * @date 2019-07-11 11:29
 * @Description 第一种方式 开启多个线程实例化去获取消息
 */
public class FirstMultConsumerThreadDemo2 {

    private static final String BROKERLIST = "hadoop102:9092";
    private static final String TOPIC = "topic-demo";
    private static final String GOURPID = "group.demo";
    private static final AtomicBoolean ISRUNNING = new AtomicBoolean(true);

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GOURPID);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaConsumerThread kafkaConsumerThread = new KafkaConsumerThread(properties, TOPIC, Runtime.getRuntime().availableProcessors());
        kafkaConsumerThread.start();
    }

    public static class KafkaConsumerThread extends Thread {

        private KafkaConsumer<String, String> kafkaConsumer;
        private ExecutorService executorService;
        private int threadNumber;

        KafkaConsumerThread(Properties properties, String topic, int threadNumber) {
            kafkaConsumer = new KafkaConsumer<>(properties);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            this.threadNumber = threadNumber;
            executorService = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        executorService.submit(new RecordsHandler(records));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }
    }

    public static class RecordsHandler extends Thread {

        public final ConsumerRecords<String, String> records;

        RecordsHandler(ConsumerRecords<String, String> records) {
            this.records = records;
        }

        @Override
        public void run() {
            // 处理 records
            for (TopicPartition tp : records.partitions()) {
                List<ConsumerRecord<String, String>> tpRecords = this.records.records(tp);
                // 处理 tpRecords
                long lastConsumerOffset = tpRecords.get(tpRecords.size() - 1).offset();

            }
        }
    }

}
