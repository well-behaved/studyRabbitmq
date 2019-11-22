package com.my.test.rabbit.publishsubscribe;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消费者：接收日志消息
 */
public class ReceiveLogs {
    /**
     * Exchange名称，在rabbitMq中生产者只能将消息发送到Exchange。
     * Exchange接收来自生产者的消息，然后将它们推入队列
     */
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //声明exchange类型
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        /**
         *创建随机名称的队列(一旦客户端断开了的连接，队列将被自动删除)
         * （对于rabbit服务器来说，生产者跟消费者都是客户端）
         * 随机生成的队列名称类似amq.gen-J4PYyOBMnky1XCGc90NfBg这样的名称，可以执行命令rabbitmqctl.bat list_queues查看
         */
        String queueName = channel.queueDeclare().getQueue();
        //对于“fanout”的“exchange” ，routingKey的值会被忽略 ，所以此处第三个参数传空字符串
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        DeliverCallback deliverCallback = (consumerTag, delivery) ->{
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" 接收到的消息为： '" + message + "'");
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag ->{});
    }


}
