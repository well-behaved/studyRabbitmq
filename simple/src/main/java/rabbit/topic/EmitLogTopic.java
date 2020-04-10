package rabbit.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 生产者，关于topic的例子
 * topic类型的exchange可以通过多个条件过滤出消费者感兴趣的消息
 * routing_key是以单词列表并且以.(点)分割开的，最大容量是255个字节
 * 单词中：*（星）代表一个单词，#(井)代表多个单词
 * 此例子中会一共生产三种类型的消息，分别为："kern.*"、"*.critical"、"aaaa.bbbbb"
 */
public class EmitLogTopic {
    private static final String EXCHANGE_NAME = "topic_logs";

    private static final List<String> routingKeyList = new ArrayList<>();

    static {
        routingKeyList.add("kern.*");
        routingKeyList.add("*.critical");
        routingKeyList.add("aaaa.bbbbb");
    }

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        /**
         *  java7新特性，在try后面的括号中创建的资源会在try中内容执行完成后自动释放，
         *  前提是这些资源必须实现java.lang.AutoCloseable接口。
         */
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            //声明exchange类型为topic
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            Random random = new Random();

            for(int i = 0; i < 100; i++){
                int num = random.nextInt(3);
                String routingKey = getRouting(num);
                String message = getMessage(num);
                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                System.out.println(" 生产者发送的消息：" + message);
                Thread.sleep(1000);
            }
        }
    }

    private static String getRouting(int num) {
        return routingKeyList.get(num);
    }

    private static String getMessage(int num) {
        return LocalDateTime.now() + ": " + routingKeyList.get(num) + "类型的消息";
    }
}
