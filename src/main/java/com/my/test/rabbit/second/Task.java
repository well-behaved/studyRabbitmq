package com.my.test.rabbit.second;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class Task {

    private final static String QUEUE_NAME = "task";

    public static void main(String[] args){

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            //为了将消息持久化（即使mq服务器挂掉也不会丢失消息）
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

            for(int i = 20; i > 0; i-- ){
                String message = "第" + (21-i) + "个任务" + getStringByNum(i);
                channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                System.out.println(" 发送 '" + message + "'");
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static String getStringByNum(int num){
        int i = 0;
        StringBuilder sb = new StringBuilder("hello");
        while(i < num){
            sb.append(".");
            i++;
        }
        return sb.toString();
    }
}
