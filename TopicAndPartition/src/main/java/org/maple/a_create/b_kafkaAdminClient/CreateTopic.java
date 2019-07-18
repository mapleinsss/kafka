package org.maple.a_create.b_kafkaAdminClient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @auther Mapleins
 * @date 2019-07-16 9:16
 * @Description 通过 adminClient 创建 topic
 */
public class CreateTopic {

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

        // 构建一个四个分区，一个副本的主题
        // 第一种方式
        NewTopic newTopic = new NewTopic(TOPIC, 4, (short) 1);
        //第二种方式
//        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        // 分区号，副本的 ids 集合
//        replicasAssignments.put(0,Arrays.asList(0));
//        replicasAssignments.put(1,Arrays.asList(0));
//        replicasAssignments.put(2,Arrays.asList(0));
//        replicasAssignments.put(3,Arrays.asList(0));
//        NewTopic newTopic = new NewTopic(TOPIC, replicasAssignments);

        // 添加 config
        Map<String,String> configs = new HashMap<>();
        configs.put("cleanup.policy","compact");
        newTopic.configs(configs);

        CreateTopicsResult topicsResult = adminClient.createTopics(Collections.singleton(newTopic));
        try {
            topicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        adminClient.close();
    }

}
