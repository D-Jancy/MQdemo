package cool.jancy.mqdemo.rocketmqtest.batch;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author dengjie
 * @version 1.0
 * @ClassName : Batchconsumer
 * @description: TODO
 * @date 2022/11/17 16:46
 */
public class Batchconsumer {

    public static void main(String[] args) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-grp5");
        consumer.setNamesrvAddr("yourIp:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 指定每次可以消费 10 条消息，默认为 1
        consumer.setConsumeMessageBatchMaxSize(10);
        // 指定每次可以从Broker拉取 40 条消息，默认为 32
        consumer.setPullBatchSize(40);

        try {
            consumer.subscribe("batch-topic", "*");
            consumer.registerMessageListener(new MessageListenerConcurrently() {

                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        System.out.println(msg.getTopic() + "\t" + msg.getQueueId() + "\t" + new String(msg.getBody()));
                    }

                    // 消费成功的返回结果
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    // 消费异常时的返回结果
                    // return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
