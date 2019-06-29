package org.maple.d_customizePartition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @auther Mapleins
 * @date 2019-06-26 16:54
 * @Description
 */
public class CustomizePartiton implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int size = partitions.size();
        if (keyBytes == null) { // 无 key
            // 此处可以做复杂的业务逻辑处理
            return counter.getAndIncrement() % size;
        } else { // 有 key
            // 此处可以做复杂的业务逻辑处理
            return Utils.toPositive(Utils.murmur2(keyBytes)) % size;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
