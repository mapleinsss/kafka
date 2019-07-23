package org.maple.a_create.test;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @auther Mapleins
 * @date 2019-07-17 16:39
 * @Description
 */
public class DemoCallback implements Callback {

    private String msg = "";

    public DemoCallback(String msg) {
        this.msg = msg;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        System.out.println(msg);
        if (exception == null) {
            System.out.println("success");
        }else {
            System.out.println(msg);
        }
    }
}
