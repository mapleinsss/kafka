package org.maple.a_create.c_validateTopic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @auther Mapleins
 * @date 2019-07-16 9:16
 * @Description 通过 adminClient 创建 topic
 */
public class CreateTopic {

    public static final String BROKERLIST = "hadoop102:9092";
    public static final String TOPIC = "topic-partitions1";

    public static Properties initProp(){
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,BROKERLIST);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,30000);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProp();
        AdminClient adminClient = AdminClient.create(properties);

        NewTopic newTopic = new NewTopic(TOPIC, 3, (short) 4);

        CreateTopicsResult topicsResult = adminClient.createTopics(Collections.singleton(newTopic));
        try {
            topicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        adminClient.close();
    }

}
