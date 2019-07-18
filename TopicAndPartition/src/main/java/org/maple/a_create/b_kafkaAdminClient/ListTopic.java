package org.maple.a_create.b_kafkaAdminClient;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @auther Mapleins
 * @date 2019-07-16 10:05
 * @Description 列出所有可用主题
 */
public class ListTopic {

    public static final String BROKERLIST = "hadoop102:9092";
    public static final String TOPIC = "topic-admin";

    public static Properties initProp() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProp();
        AdminClient adminClient = AdminClient.create(properties);
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        try {
            Set<String> set = listTopicsResult.names().get();
            set.forEach(System.out::println);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        adminClient.close();
    }

}
