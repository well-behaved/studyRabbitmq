package rabbit.first;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import java.io.IOException;

public class Consumer {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception  {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println("等待消息");

//        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
//            String message = new String(delivery.getBody(), "UTF-8");
//            System.out.println(" 接收到消息：" + message );
//        };


        DeliverCallback deliverCallback = new DeliverCallback(){
            @Override
            public void handle(String consumerTag, Delivery delivery) throws IOException{
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" 接收到的消息为： '" + message + "'");
            }
        };

        CancelCallback cancelCallback = new CancelCallback(){
            @Override
            public void handle(String consumerTag) throws IOException {

            }
        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, cancelCallback);
    }
}
