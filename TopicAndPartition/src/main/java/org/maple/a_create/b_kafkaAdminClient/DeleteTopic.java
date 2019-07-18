package org.maple.a_create.b_kafkaAdminClient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @auther Mapleins
 * @date 2019-07-16 10:27
 * @Description delete topic 逻辑删除
 */
public class DeleteTopic {

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
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singleton(TOPIC));
        try {
            deleteTopicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        adminClient.close();
    }
}
