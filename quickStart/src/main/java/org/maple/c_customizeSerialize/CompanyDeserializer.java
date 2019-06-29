package org.maple.c_customizeSerialize;

import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

/**
 * @auther Mapleins
 * @date 2019-06-25 16:51
 * @Description
 */
public class CompanyDeserializer implements Deserializer<Company> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Company deserialize(String topic, byte[] bytes) {
        return (Company) SerializationUtils.deserialize(bytes);
    }

    @Override
    public void close() {

    }
}
