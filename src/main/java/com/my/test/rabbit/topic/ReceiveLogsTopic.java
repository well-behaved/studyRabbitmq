package com.my.test.rabbit.topic;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * 消费者,关于topic的例子
 * topic类型的exchange可以通过多个条件过滤出消费者感兴趣的消息
 * routing_key是以单词列表并且以.(点)分割开的，最大容量是255个字节
 * 单词中：*（星）代表一个单词，#(井)代表多个单词
 * 此例子中表示只对"kern.*"、"*.critical"这两种路由类型的消息感兴趣，
 * 除这两种类型的数据，之外的数据都不会存入到队列中
 */
public class ReceiveLogsTopic {

    /**
     * Exchange名称，在rabbitMq中生产者只能将消息发送到Exchange。
     * Exchange接收来自生产者的消息，然后将它们推入队列
     */
    private static final String EXCHANGE_NAME = "topic_logs";

    private static final List<String> routingKeyList = new ArrayList<>();
    static {
        routingKeyList.add("kern.*");
        routingKeyList.add("*.critical");
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //声明exchange类型为topic
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        /**
         *创建随机名称的队列(一旦生产者断开了的连接，队列将被自动删除)
         * 随机生成的队列名称类似amq.gen-J4PYyOBMnky1XCGc90NfBg这样的名称，
         * 可以执行命令rabbitmqctl.bat list_queues查看
         */
        String queueName = channel.queueDeclare().getQueue();

        for(String bindingKey : routingKeyList){
            channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
        }

        DeliverCallback deliverCallback = (consumerTag, delivery) ->{
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" 接收到的消息为： '" + message + "'");
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag ->{});
    }
}
