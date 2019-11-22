package com.my.test.rabbit.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * 客户端
 * 基于rabbitMq实现的rpc调用
 * 首先请求会发送到“rpc_queue”对列中
 * 服务端消费“rpc_queue”队列消息，并且把结果返回给客户端生成的“临时队列”中
 * 客户端消费“临时队列”中的消息，并且根据请求唯一的ID判断是否为自己请求的响应消息
 * 最终返回响应结果
 */
public class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] argv) {
        try (RPCClient fibonacciRpc = new RPCClient()) {
            for (int i = 0; i < 32; i++) {
                String i_str = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + i_str + ")");
                String response = fibonacciRpc.call(i_str);
                System.out.println(" [.] Got '" + response + "'");
                Thread.sleep(1000);
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String call(String message) throws IOException, InterruptedException {
        //标识请求的唯一ID
        final String corrId = UUID.randomUUID().toString();

        /**
         * 生成一个随机队列，用来存放响应的消息（远程系统返回的结果）
         * 此处每次请求都会生成一个新的“临时队列”，可以在main中定义返回队列（目的是客户端
         * 只用一个“响应队列”即可，没必要每次都创建）
         */
        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        //发送请求到“请求队列”
        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

        //创建大小为1的阻塞队列
        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        //消费“响应队列”
        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });
        //获取到对应请求的值,然后返回，注意take方法阻塞
        String result = response.take();
        //删除生成的临时队列
        channel.basicCancel(ctag);
        return result;
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
