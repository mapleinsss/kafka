package org.maple.a_create.b_kafkaAdminClient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @auther Mapleins
 * @date 2019-07-16 10:39
 * @Description
 */
public class AlterConfig {

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
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);
        ConfigEntry compact = new ConfigEntry("cleanup.policy", "compact");
        Config config = new Config(Collections.singleton(compact));
        Map<ConfigResource,Config> configs = new HashMap<>();
        configs.put(resource,config);
        AlterConfigsResult result = adminClient.alterConfigs(configs);
        try {
            result.all().get();
    } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
    }

    }
}
