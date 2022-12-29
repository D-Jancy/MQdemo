package cool.jancy.mqdemo.rocketmqtest.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dengjie
 * @version 1.0
 * @ClassName : BatchProducer
 * @description: TODO
 * @date 2022/11/17 16:46
 */
public class BatchProducer {

    public static void main(String[] args) {

        DefaultMQProducer producer = new DefaultMQProducer("producer-grp5");
        producer.setNamesrvAddr("yourIp:9876");

        // 指定要发送的消息的最大大小，默认是4M
        // 不过，仅修改该属性是不行的，还需要同时修改broker加载的配置文件中的maxMessageSize属性
        // producer.setMaxMessageSize(8 * 1024 * 1024);

        try {
            producer.start();
            List<Message> messages = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                byte[] body = ("Hi," + i).getBytes();
                Message msg = new Message("batch-topic", "jancy", body);
                messages.add(msg);
            }
            // 定义消息列表分割器，将消息列表分割为多个不超出4M大小的小列表
            MessageListSplitter splitter = new MessageListSplitter(messages);
            while (splitter.hasNext()) {
                List<Message> listItem = splitter.next();
                producer.send(listItem);

            }
            producer.shutdown();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
