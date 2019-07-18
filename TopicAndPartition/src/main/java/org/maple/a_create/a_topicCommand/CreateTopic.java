package org.maple.a_create.a_topicCommand;

/**
 * @auther Mapleins
 * @date 2019-07-15 11:29
 * @Description 创建主题
 */
public class CreateTopic {
    public static void main(String[] args) {
        String[] options = new String[]{
                "--zookeeper","hadoop102:2181",
                "--create",
                "--topic","topic-datax",
                "--partitions","4",
                "--replication-factor","2"
        };
        kafka.admin.TopicCommand.main(options);
    }
}
