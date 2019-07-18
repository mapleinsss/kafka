package org.maple.a_create.b_kafkaAdminClient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @auther Mapleins
 * @date 2019-07-16 9:57
 * @Description 描述主题
 */
public class DescribeTopic {

    public static final String BROKERLIST = "hadoop102:9092";
    public static final String TOPIC = "topic-admin";

    public static Properties initProp(){
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,BROKERLIST);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,30000);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProp();
        AdminClient adminClient = AdminClient.create(properties);
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(TOPIC));
        try {
            Map<String, TopicDescription> map = describeTopicsResult.all().get();
            TopicDescription topicDescription = map.get(TOPIC);
            List<TopicPartitionInfo> partitionInfos = topicDescription.partitions();
            partitionInfos.forEach(System.out::println);

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        adminClient.close();
    }
}
