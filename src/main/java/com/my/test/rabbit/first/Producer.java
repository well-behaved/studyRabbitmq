package com.my.test.rabbit.first;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.io.IOException;

public class Producer {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args){

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            for(int i = 0; i < 100; i++){
                channel.basicPublish("", QUEUE_NAME, null, (i+"").getBytes());
                System.out.println(" 发送消息：'" + (i+""));
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
