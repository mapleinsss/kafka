package org.maple.a_create.b_kafkaAdminClient;

import org.apache.kafka.clients.admin.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @auther Mapleins
 * @date 2019-07-16 10:43
 * @Description 增加分区
 */
public class AddPartition {

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
        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        Map<String, NewPartitions> map = new HashMap<>();
        map.put(TOPIC, newPartitions);
        CreatePartitionsResult result = adminClient.createPartitions(map);
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

}
