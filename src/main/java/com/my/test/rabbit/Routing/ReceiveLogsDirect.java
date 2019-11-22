package com.my.test.rabbit.Routing;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消费者
 * 生产者会产生error跟info两种类型数据，消费者只关心error类型的数据
 */
public class ReceiveLogsDirect {

    /**
     * Exchange名称，在rabbitMq中生产者只能将消息发送到Exchange。
     * Exchange接收来自生产者的消息，然后将它们推入队列
     */
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //声明exchange类型为direct
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        /**
         *创建随机名称的队列(一旦生产者断开了的连接，队列将被自动删除)
         * 随机生成的队列名称类似amq.gen-J4PYyOBMnky1XCGc90NfBg这样的名称，
         * 可以执行命令rabbitmqctl.bat list_queues查看
         */
        String queueName = channel.queueDeclare().getQueue();
        //只关心error类型的数据
        String routingKey = "error";
        /**
         * 可以同时绑定多种关心的类型数据
         * 例如在下面添加如下代码：
         * channel.queueBind(queueName, EXCHANGE_NAME, "info");
         * 即会同时绑定info跟error
         */
        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);

        DeliverCallback deliverCallback = (consumerTag, delivery) ->{
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" 接收到的消息为： '" + message + "'");
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag ->{});
    }
}
