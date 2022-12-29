package cool.jancy.mqdemo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class RocketMQdemoApplicationTests {

    private static final String NAMESRV_ADDR = "yourIp:9876";

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @Test
    public void syncProducer() {

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer();
        defaultMQProducer.setProducerGroup("producer-grp1");
        defaultMQProducer.setNamesrvAddr(NAMESRV_ADDR);
        // 设置当发送失败时重试发送的次数，默认为 2 次
        defaultMQProducer.setRetryTimesWhenSendFailed(3);
        // 设置发送超时时限为5s，默认3s
        defaultMQProducer.setSendMsgTimeout(60000);
        try {
            defaultMQProducer.start();

            Message message = new Message("sync-topic", "jancy", "同步消息".getBytes());
            //发送消息并得到消息的发送结果，然后打印
            SendResult sendResult = defaultMQProducer.send(message);
            System.out.printf("%s%n", sendResult);

            //关闭生产者
            defaultMQProducer.shutdown();
            System.out.println("producer shutdown");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void normalConsumer() {

        //注意及时关闭，避免while true导致堆栈溢出
        while (true) {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-grp1");
            consumer.setNamesrvAddr(NAMESRV_ADDR);
            try {
                //同步消息订阅
                consumer.subscribe("sync-topic", "*");
                //异步消息订阅
                consumer.subscribe("async-topic", "*");
                //单向消息订阅
                consumer.subscribe("oneway-topic", "*");

                //consumer.setMessageListener(new MessageListenerConcurrently() {
                //
                //    @Override
                //    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                //                                                    ConsumeConcurrentlyContext context) {
                //        for (MessageExt msg : msgs) {
                //            System.out.printf("消费消息:%s", new String(msg.getBody()) + "\n");
                //        }
                //
                //        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                //    }
                //});
                consumer.registerMessageListener(new MessageListenerConcurrently() {

                    @Override
                    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                        for (MessageExt msg : msgs) {
                            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msg.getBody()));
                        }

                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                });
                consumer.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void asyncProducer() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-grp1");
        producer.setNamesrvAddr(NAMESRV_ADDR);
        // 指定异步发送失败后不进行重试发送
        producer.setRetryTimesWhenSendFailed(0);
        // 指定新创建的Topic的Queue数量为 2 ，默认为 4
        producer.setDefaultTopicQueueNums(2);

        try {
            producer.start();
            Message message = new Message("async-topic", "jancy", "异步消息".getBytes());
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    throwable.printStackTrace();
                }
            });

            // sleep一会儿
            // 由于采用的是异步发送，所以若这里不sleep，
            // 则消息还未发送就会将producer给关闭，报错
            TimeUnit.SECONDS.sleep(3);
            producer.shutdown();
            System.out.println("producer shutdown");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void onewayProducer() {

        DefaultMQProducer producer = new DefaultMQProducer("producer-grp1");
        producer.setNamesrvAddr(NAMESRV_ADDR);

        try {
            producer.start();
            Message message = new Message("oneway-topic", "jancy", "单向消息".getBytes());
            producer.sendOneway(message);

            producer.shutdown();
            System.out.println("producer shutdown");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void someOrderPruducer1() {
        //需要提前创建好Topic
        DefaultMQProducer producer = new DefaultMQProducer("producer-grp2");
        producer.setNamesrvAddr(NAMESRV_ADDR);

        try {
            producer.start();
            final List<MessageQueue> messageQueues = producer.fetchPublishMessageQueues("some-order-topic1");
            Message message = null;
            MessageQueue messageQueue = null;

            for (int i = 0; i < 100; i++) {
                // 采用轮询的方式指定MQ，发送订单消息，保证同一个订单的消息按顺序
                // 发送到同一个MQ
                messageQueue = messageQueues.get(i % 8);


                message = new Message("some-order-topic1", ("hello rocketmq order create - " + i).getBytes());
                producer.send(message, messageQueue);

                message = new Message("some-order-topic1", ("hello rocketmq order pay - " + i).getBytes());
                producer.send(message, messageQueue);

                message = new Message("some-order-topic1", ("hello rocketmq order delivery - " + i).getBytes());
                producer.send(message, messageQueue);
            }
            producer.shutdown();
            System.out.println("producer shutdown");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void orderConsumer() {

        //注意及时关闭，避免while true导致堆栈溢出
        while (true) {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-grp2");
            consumer.setNamesrvAddr(NAMESRV_ADDR);
            try {
                //分区有序的第一种方式，需提前创建Topic
                consumer.subscribe("some-order-topic1", "*");
                //分区有序的第二种方式
                //consumer.subscribe("some-order-topic2", "*");
                //全局有序
                //consumer.subscribe("final-order-topic", "*");

                consumer.setConsumeThreadMin(1);
                consumer.setConsumeThreadMax(1);
                consumer.setPullBatchSize(1);
                consumer.setConsumeMessageBatchMaxSize(1);

                //consumer.setMessageListener(new MessageListenerConcurrently() {
                //
                //    @Override
                //    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                //                                                    ConsumeConcurrentlyContext context) {
                //        for (MessageExt msg : msgs) {
                //            System.out.printf("消费消息:%s", new String(msg.getBody()) + "\n");
                //        }
                //
                //        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                //    }
                //});
                consumer.registerMessageListener(new MessageListenerConcurrently() {

                    @Override
                    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                        for (MessageExt msg : msgs) {
                            System.out.println(msg.getTopic() + "\t" + msg.getQueueId() + "\t" + new String(msg.getBody()));
                        }

                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                });
                consumer.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void someOrderProduder2() {

        DefaultMQProducer producer = new DefaultMQProducer("producer-grp2");
        producer.setNamesrvAddr(NAMESRV_ADDR);

        try {
            producer.start();
            for (int i = 0; i < 100; i++) {
                Integer orderId = i;
                byte[] body = ("Hi," + i).getBytes();
                Message msg = new Message("some-order-topic2", "jancy", body);
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer id = (Integer) arg;
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, orderId);
                System.out.println(sendResult);
            }
            producer.shutdown();
            System.out.println("producer shutdown");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void finalOrderProduder() {

        DefaultMQProducer producer = new DefaultMQProducer("producer-grp2");
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.setDefaultTopicQueueNums(1);

        try {
            producer.start();
            for (int i = 0; i < 100; i++) {
                byte[] body = ("Hi," + i).getBytes();
                Message msg = new Message("final-order-topic", "jancy", body);
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            }
            producer.shutdown();
            System.out.println("producer shutdown");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void delayProducer() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-grp3");
        producer.setNamesrvAddr(NAMESRV_ADDR);
        try {
            producer.start();
            for (int i = 0; i < 10; i++) {
                byte[] body = ("Hi," + i).getBytes();
                Message msg = new Message("delay-topic", "jancy", body);
                // 指定消息延迟等级为 3 级，即延迟10s
                msg.setDelayTimeLevel(3);
                SendResult sendResult = producer.send(msg);
                // 输出消息被发送的时间
                System.out.print(new SimpleDateFormat("mm:ss").format(System.currentTimeMillis()) + "\n");
                System.out.println(" ," + sendResult);
            }
            producer.shutdown();
            System.out.println("producer shutdown");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void delayConsumer() {
        while (true) {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-grp3");
            consumer.setNamesrvAddr(NAMESRV_ADDR);
            try {
                consumer.subscribe("delay-topic", "*");
                // 指定从第一条消息开始消费
                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
                consumer.registerMessageListener(new MessageListenerConcurrently() {

                    @Override
                    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                        for (MessageExt msg : msgs) {
                            System.out.print(new SimpleDateFormat("mm:ss").format(System.currentTimeMillis()) + "\n");
                            System.out.println(msg.getTopic() + "\t" + msg.getQueueId() + "\t" + new String(msg.getBody()));
                        }

                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                });
                consumer.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 事务消息生产者需要一直运行，不好放入test类中，因为使用while循环的话会一直发送消息
     *
     * @Test public void transactionProducer() {
     * <p>
     * //while (true) {
     * TransactionMQProducer producer = new TransactionMQProducer("producer-grp4");
     * producer.setNamesrvAddr(NAMESRV_ADDR);
     * <p>
     * //定义事务监听器
     * TransactionListener listener = new TransactionListener() {
     * <p>
     * // 回调操作方法
     * // 消息预提交成功就会触发该方法的执行，用于完成本地事务
     * @Override public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
     * System.out.println("预提交消息成功：" + msg);
     * // 假设的事务成功与否的判断逻辑
     * // 假设接收到TAGA的消息就表示扣款操作成功，TAGB的消息表示扣款失败，TAGC表示扣款结果不清楚，需要执行消息回查
     * if (StringUtils.equals("TAGA", msg.getTags())) {
     * return LocalTransactionState.COMMIT_MESSAGE;
     * } else if (StringUtils.equals("TAGB", msg.getTags())) {
     * return LocalTransactionState.ROLLBACK_MESSAGE;
     * } else if (StringUtils.equals("TAGC", msg.getTags())) {
     * return LocalTransactionState.UNKNOW;
     * }
     * return LocalTransactionState.UNKNOW;
     * }
     * <p>
     * // 消息回查方法
     * // 引发消息回查的原因最常见的有两个：
     * // 1)回调操作返回UNKNWON
     * // 2)TC没有接收到TM的最终全局事务确认指令
     * @Override public LocalTransactionState checkLocalTransaction(MessageExt msg) {
     * System.out.println("执行消息回查" + msg.getTags());
     * //按照回调操作方法逻辑思路修改进行回查，但demo为了便捷体现事务能回调直接返回成功
     * return LocalTransactionState.COMMIT_MESSAGE;
     * }
     * };
     * <p>
     * //定义生产者线程池
     * ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new
     * ThreadFactory() {
     * @Override public Thread newThread(Runnable runnable) {
     * Thread thread = new Thread(runnable);
     * thread.setName("client-transaction-msg-check-thread");
     * return thread;
     * }
     * });
     * <p>
     * try {
     * producer.start();
     * producer.setTransactionListener(listener);
     * producer.setExecutorService(threadPoolExecutor);
     * <p>
     * String[] tags = {"TAGA", "TAGB", "TAGC"};
     * for (int i = 0; i < 3; i++) {
     * byte[] body = ("Hi," + i).getBytes();
     * Message msg = new Message("transact-topic", tags[i], body);
     * // 发送事务消息
     * // 第二个参数用于指定在执行本地事务时要使用的业务参数，即executeLocalTransaction()中的object参数
     * SendResult sendResult = producer.sendMessageInTransaction(msg, null);
     * System.out.println("发送结果为：" + sendResult.getSendStatus());
     * }
     * <p>
     * //需要回查所以不能停止producer
     * //producer.shutdown();
     * //System.out.println("producer shutdown");
     * } catch (Exception e) {
     * e.printStackTrace();
     * }
     * //}
     * }
     * @Test public void transactionConsumer() {
     * <p>
     * while (true) {
     * DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-grp4");
     * consumer.setNamesrvAddr(NAMESRV_ADDR);
     * // 指定从第一条消息开始消费
     * consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
     * // 指定采用“广播模式”进行消费，默认为“集群模式”
     * // consumer.setMessageModel(MessageModel.BROADCASTING);
     * try {
     * consumer.subscribe("transact-topic", "*");
     * consumer.registerMessageListener(new MessageListenerConcurrently() {
     * @Override public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
     * ConsumeConcurrentlyContext context) {
     * for (MessageExt msg : msgs) {
     * System.out.println(
     * msg.getTopic() + "\t" +
     * msg.getQueueId() + "\t" +
     * new String(msg.getBody()));
     * }
     * <p>
     * return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
     * }
     * });
     * consumer.start();
     * } catch (Exception e) {
     * e.printStackTrace();
     * }
     * }
     * }
     */


    @Test
    public void filterByTagProducer1() {

        DefaultMQProducer producer = new DefaultMQProducer("producer-grp6");
        producer.setNamesrvAddr(NAMESRV_ADDR);
        try {
            producer.start();

            for (int i = 0; i < 100; i++) {
                byte[] body = ("Hi" + i).getBytes();
                Message message = new Message("tag-filter-topic", "tag-" + i % 3, body);
                SendResult sendResult = producer.send(message);
                System.out.println(" ," + sendResult);
            }
            producer.shutdown();
            System.out.println("producer shutdown");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void filterByTagProducer2() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-grp6");
        producer.setNamesrvAddr(NAMESRV_ADDR);
        try {
            producer.start();

            String[] tags = {"MyTagA", "MyTagB", "MyTagC"};
            for (int i = 0; i < 100; i++) {
                byte[] body = ("Hi" + i).getBytes();
                String tag = tags[i % tags.length];
                Message message = new Message("tag-filter-topic", tag, body);
                SendResult sendResult = producer.send(message);
                System.out.println("," + sendResult);
            }
            producer.shutdown();
            System.out.println("producer shutdown");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void filterByTagConsumer() {
        while (true) {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-grp6");
            consumer.setNamesrvAddr(NAMESRV_ADDR);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            try {
                //producer1
                //consumer.subscribe("tag-filter-topic", "tag-0");
                //producer2
                consumer.subscribe("tag-filter-topic", "MyTagA||MyTagB");
                consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                    for (MessageExt msg : msgs) {
                        System.out.print(new SimpleDateFormat("mm:ss").format(System.currentTimeMillis()) + "\n");
                        System.out.println(msg.getTopic() + "\t" + msg.getQueueId() + "\t" + new String(msg.getBody()));
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                });
                consumer.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void filterBySqlProducer() {
        DefaultMQProducer producer = new DefaultMQProducer("producer-grp6");
        producer.setNamesrvAddr(NAMESRV_ADDR);
        try {
            producer.start();
            for (int i = 0; i < 20; i++) {
                byte[] body = ("Hi " + i).getBytes();
                Message message = new Message("sql-filter-topic", "jancy", body);
                message.putUserProperty("age", i + "");
                SendResult sendResult = producer.send(message);
                System.out.println(sendResult);
            }
            producer.shutdown();
            System.out.println("producer shutdown");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void filterBySqlConsumer() {
        while (true) {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-grp6");
            consumer.setNamesrvAddr(NAMESRV_ADDR);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            try {
                consumer.subscribe("sql-filter-topic", MessageSelector.bySql("age between 0 and 7"));
                consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                    for (MessageExt msg : msgs) {
                        System.out.print(new SimpleDateFormat("mm:ss").format(System.currentTimeMillis()) + "\n");
                        System.out.println(msg.getTopic() + "\t" + msg.getQueueId() + "\t" + new String(msg.getBody()));
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                });
                consumer.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void springbootNormalProducer() throws InterruptedException {
        rocketMQTemplate.convertAndSend("normal-topic", "hello");
        Thread.sleep(30000);
    }
}