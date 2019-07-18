package org.maple.a_create.a_topicCommand;



/**
 * @auther Mapleins
 * @date 2019-07-16 9:04
 * @Description
 */
public class DescribeTopic {
    public static void main(String[] args) {
        String[] options = new String[]{
                "--zookeeper", "hadoop102:2181",
                "--describe",
                "--topic","topic-create-api"
        };
        kafka.admin.TopicCommand.main(options);
    }
}
