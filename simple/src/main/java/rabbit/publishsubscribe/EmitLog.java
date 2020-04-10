package rabbit.publishsubscribe;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 生产者，生产日志信息
 */
public class EmitLog {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        /**
         *  java7新特性，在try后面的括号中创建的资源会在try中内容执行完成后自动释放，
         *  前提是这些资源必须实现java.lang.AutoCloseable接口。
         */
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            //声明exchange类型为fanout
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            for(int i = 0; i < 200; i++){
                String message = i+"";
                channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
                System.out.println(" 生产者发送的消息：" + message);
            }


        }
    }
}
