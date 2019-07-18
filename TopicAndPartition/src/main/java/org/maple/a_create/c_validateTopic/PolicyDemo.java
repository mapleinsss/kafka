package org.maple.a_create.c_validateTopic;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;
import java.util.Objects;

/**
 * @auther Mapleins
 * @date 2019-07-16 11:05
 * @Description 通过该类来验证主题的合法性
 */
public class PolicyDemo implements CreateTopicPolicy {

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        // 1. 非空判断，这里就不判断了
        // 2. 判断分区数,副本因子，主题，配置等...
        if (requestMetadata.numPartitions() < 5) {
            throw new PolicyViolationException("Topic should have at least 5 partitions,received :" + requestMetadata.numPartitions());
        }
        if (requestMetadata.replicationFactor() <= 1) {
            throw new PolicyViolationException("Topic should have at least 2 replication,received :" + requestMetadata.replicationFactor());
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
