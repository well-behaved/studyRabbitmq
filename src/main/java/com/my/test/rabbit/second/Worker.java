package com.my.test.rabbit.second;

import com.rabbitmq.client.*;

import java.io.IOException;

public class Worker {
    private final static String QUEUE_NAME = "task";

    public static void main(String[] args) throws Exception  {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        System.out.println("等待消息");
        //以下两行代码必须在发送者跟消费者都有（参考官方文档）
        boolean durable = true;
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
        //在处理并确认上一条消息之前，不要将新消息发送给此worker。而是将其分派给不忙的下一个worker。
        //（简单的讲，就是每次只载入一条消息）
        int prefetchCount = 1 ;
        channel.basicQos(prefetchCount);

        DeliverCallback deliverCallback = new DeliverCallback(){
            public void handle(String consumerTag, Delivery delivery) throws IOException{
                String message = new String(delivery.getBody(), "UTF-8");

                System.out.println(" [x] Received '" + message + "'");
                try {
                    doWork(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    System.out.println(" [x] Done");
                    //消息消费完毕，发送ack确认（只有确认之后，mq服务器才会从队列中删除消息）
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            }
        };
        //把自动确认关闭，结合上面channel.basicAck使用（忘记的时候看官方文档）
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch: task.toCharArray()) {
            if (ch == '.') Thread.sleep(100);
        }
    }
}
