package cool.jancy.mqdemo;

import com.rabbitmq.client.*;
import cool.jancy.mqdemo.common.RabbitMQConnection;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@SpringBootTest
class RabbitMQdemoApplicationTests {

    RabbitMQdemoApplicationTests() throws IOException, TimeoutException {
    }

    @Test
    void contextLoads() {
    }

    Connection connection = RabbitMQConnection.getConnection();
    Channel channel = connection.createChannel();

    @Test
    public void sendSimple() throws IOException, TimeoutException {

        channel.queueDeclare("simple_queue", false, false, false, null);
        channel.basicPublish("", "simple_queue", null, "simple_message".getBytes());
        channel.close();
        connection.close();
    }

    @Test
    public void consumeSimple() throws IOException, TimeoutException {
        channel.basicConsume("simple_queue", true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body);
                System.out.println(msg);
            }
        });
        channel.close();
        connection.close();
    }

    @Test
    public void sendWork() throws IOException, TimeoutException {
        channel.queueDeclare("work_queue", true, false, false, null);
        for (int i = 0; i < 100; i++) {
            channel.basicPublish("", "work_queue", MessageProperties.PERSISTENT_TEXT_PLAIN, ("work_message" + i).getBytes());
        }
        channel.close();
        connection.close();
    }


    @Test
    public void consumeWork1() throws IOException, TimeoutException {
        while (true) {
            channel.basicConsume("work_queue", true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body);
                    System.out.println(msg);
                }
            });
        }
    }

    @Test
    public void consumeWork2() throws IOException, TimeoutException {
        while (true) {
            channel.basicConsume("work_queue", true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body);
                    System.out.println(msg);
                }
            });
        }
    }

    @Test
    public void sendSub() throws IOException, TimeoutException {
        channel.exchangeDeclare("exchange_fanout", BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare("sub_queue1", true, false, false, null);
        channel.queueDeclare("sub_queue2", true, false, false, null);

        channel.queueBind("sub_queue1", "exchange_fanout", "sub");
        channel.queueBind("sub_queue2", "exchange_fanout", "sub1");

        for (int i = 0; i < 100; i++) {
            channel.basicPublish("exchange_fanout", "sub", MessageProperties.PERSISTENT_TEXT_PLAIN, ("sub_message" + i).getBytes());
        }
        channel.close();
        connection.close();
    }

    @Test
    public void consumeSub1() throws IOException, TimeoutException {
        while (true) {
            channel.basicConsume("sub_queue1", true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body);
                    System.out.println(msg);
                }
            });
        }
    }

    @Test
    public void consumeSub2() throws IOException, TimeoutException {
        while (true) {
            channel.basicConsume("sub_queue2", true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body);
                    System.out.println(msg);
                }
            });
        }
    }

    @Test
    public void sendRoute() throws IOException, TimeoutException {
        channel.exchangeDeclare("exchange_route", BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare("route_queue1", true, false, false, null);
        channel.queueDeclare("route_queue2", true, false, false, null);

        channel.queueBind("route_queue1", "exchange_route", "route1");
        channel.queueBind("route_queue2", "exchange_route", "route2");

        for (int i = 0; i < 100; i++) {
            channel.basicPublish("exchange_route", "route1", MessageProperties.PERSISTENT_TEXT_PLAIN, ("route_message" + i).getBytes());
        }
        channel.close();
        connection.close();
    }

    @Test
    public void consumeRoute1() throws IOException, TimeoutException {
        while (true) {
            channel.basicConsume("route_queue1", true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body);
                    System.out.println(msg);
                }
            });
        }
    }

    @Test
    public void consumeRoute2() throws IOException, TimeoutException {
        while (true) {
            channel.basicConsume("route_queue2", true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body);
                    System.out.println(msg);
                }
            });
        }
    }


    @Test
    public void sendTopic() throws IOException, TimeoutException {
        channel.exchangeDeclare("exchange_topic", BuiltinExchangeType.TOPIC, true);
        channel.queueDeclare("topic_queue1", true, false, false, null);
        channel.queueDeclare("topic_queue2", true, false, false, null);

        channel.queueBind("topic_queue1", "exchange_topic", "#.topic1.#");
        channel.queueBind("topic_queue2", "exchange_topic", "#.topic2.#");

        for (int i = 0; i < 100; i++) {
            channel.basicPublish("exchange_topic", "topic1.topic2", MessageProperties.PERSISTENT_TEXT_PLAIN, ("topic_message" + i).getBytes());
        }
        channel.close();
        connection.close();
    }

    @Test
    public void consumeTopic1() throws IOException, TimeoutException {
        while (true) {
            channel.basicConsume("topic_queue1", true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body);
                    System.out.println(msg);
                }
            });
        }
    }

    @Test
    public void consumeTopic2() throws IOException, TimeoutException {
        while (true) {
            channel.basicConsume("topic_queue2", true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body);
                    System.out.println(msg);
                }
            });
        }
    }

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Test

    public void springPublisher() {

        //设置确认机制，确保生产者是否将消息发送至交换机
        rabbitTemplate.setConfirmCallback(new RabbitTemplate.ConfirmCallback() {
            @Override
            public void confirm(CorrelationData correlationData, boolean b, String s) {
                if (b) {
                    System.out.println("confirm接受成功!");
                } else {
                    System.out.println("confirm接受失败，原因 为：" + s + "ID: " + correlationData.getId());
                    // 做一些处理。
                }
            }
        });

        //设置退回机制，确保交换机是否将消息发送至队列
        rabbitTemplate.setReturnsCallback(new RabbitTemplate.ReturnsCallback() {
            @Override
            public void returnedMessage(ReturnedMessage returnedMessage) {
                System.out.println("消息对象：" + returnedMessage.getMessage());
                System.out.println("错误码：" + returnedMessage.getReplyCode());
                System.out.println("错误信息：" + returnedMessage.getReplyText());
                System.out.println("交换机：" + returnedMessage.getExchange());
                System.out.println("路由键：" + returnedMessage.getRoutingKey());
                // 做一些处理.
            }
        });

        /*
        //重试机制如何实现未知
        rabbitTemplate.setRecoveryCallback(new RecoveryCallback<Object>() {
            @Override
            public Object recover(RetryContext retryContext) throws Exception {
                return null;
            }
        });
        */

        //过期时间消息
        org.springframework.amqp.core.MessageProperties messageProperties =
                new org.springframework.amqp.core.MessageProperties();
        //设置存活时间
        messageProperties.setExpiration("5000");
        // 创建消息对象
        Message message1 = new Message("send message...".getBytes(), messageProperties);

        Message message2 = new Message("11111111111".getBytes());

        for (int i = 0; i < 10; i++) {
            //rabbitTemplate.convertAndSend("normal_exchange", "normal", message2);
            rabbitTemplate.convertAndSend("normal_exchange", "normal", message1);
        }
    }
}
