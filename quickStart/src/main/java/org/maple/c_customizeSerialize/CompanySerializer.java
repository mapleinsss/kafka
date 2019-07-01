package org.maple.c_customizeSerialize;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @auther Mapleins
 * @date 2019-06-25 16:51
 * @Description
 */
public class CompanySerializer implements Serializer<Company> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        return SerializationUtils.serialize(data);
    }

    @Override
    public void close() {

    }
}
