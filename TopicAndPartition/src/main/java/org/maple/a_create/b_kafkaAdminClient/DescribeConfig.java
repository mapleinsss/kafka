package org.maple.a_create.b_kafkaAdminClient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @auther Mapleins
 * @date 2019-07-16 10:30
 * @Description 列出主题中所有的配置信息
 */
public class DescribeConfig {

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
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);
        DescribeConfigsResult result = adminClient.describeConfigs(Collections.singleton(configResource));
        try {
            Config config = result.all().get().get(configResource);
            System.out.println(config);
            adminClient.close();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

}
