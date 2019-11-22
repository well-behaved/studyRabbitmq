package com.my.test.rabbit.Routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Random;

/**
 * 生产者，关于路由的例子
 * 创建两种日志类型，发送给exchange，消费者只关心error类型的日志
 */
public class EmitLogDirect {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        /**
         *  java7新特性，在try后面的括号中创建的资源会在try中内容执行完成后自动释放，
         *  前提是这些资源必须实现java.lang.AutoCloseable接口。
         */
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            //声明exchange类型为Direct
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");
            Random random = new Random();
            for(int i = 0; i < 10; i++){
                int num = random.nextInt(100);
                String message = num + "";
                String routingKey = getSeverity(num);
                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                System.out.println(" 生产者发送的消息：" + message);
            }
        }
    }

    private static String getSeverity(int num) {
        /**
         * 假设随机数是3的倍数的时候，认为打印得到info日志（
         * 其余情况是info日志
         * （官方的例子太麻烦，这里简化了）
         */
        if (num % 3 == 0){
            return "error";
        }
        return "info";
    }
}
