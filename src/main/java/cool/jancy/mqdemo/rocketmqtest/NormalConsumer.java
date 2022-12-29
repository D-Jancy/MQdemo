package cool.jancy.mqdemo.rocketmqtest;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author dengjie
 * @version 1.0
 * @ClassName : NormalProducer
 * @description: TODO
 * @date 2022/12/29 15:25
 */
@Component
@RocketMQMessageListener(consumerGroup = "jancyConsumer", topic = "normal-topic")
public class NormalConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String s) {
        System.out.println("处理消息，" + s);
    }
}
