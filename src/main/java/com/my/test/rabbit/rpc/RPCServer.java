package com.my.test.rabbit.rpc;

import com.rabbitmq.client.*;

/**
 * 服务端
 * 基于rabbitMq实现的rpc调用
 * 首先请求会发送到“rpc_queue”对列中
 * 服务端消费“rpc_queue”队列消息，并且把结果返回给客户端生成的“临时队列”中
 * 客户端消费“临时队列”中的消息，并且根据请求唯一的ID判断是否为自己请求的响应消息
 * 最终返回响应结果
 */
public class RPCServer {

    private static final String RPC_QUEUE_NAME = "rpc_queue";

    /**
     * 斐波那契数
     * @param n 第几个位置
     * @return n位置上面值
     */
    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] args) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try(Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()){
            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
            channel.queuePurge(RPC_QUEUE_NAME);
            channel.basicQos(1);

            System.out.println("等待rpc请求。。。。");
            Object monitor = new Object();
            DeliverCallback deliverCallback = (consumerTag, delivery)->{
                //获取请求的唯一标识ID，并且在返回给“响应队列”的时候带回去
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();

                String response = "";

                try {
                    String message = new String(delivery.getBody(), "UTF-8");
                    int n = Integer.parseInt(message);

                    System.out.println(" [.] fib(" + message + ")");
                    response += fib(n);
                } catch (RuntimeException e) {
                    System.out.println(" [.] " + e.toString());
                } finally {
                    channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    // RabbitMq consumer worker thread notifies the RPC server owner thread
                    synchronized (monitor) {
                        monitor.notify();
                    }
                }
            };

            channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> { }));
            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized (monitor) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
