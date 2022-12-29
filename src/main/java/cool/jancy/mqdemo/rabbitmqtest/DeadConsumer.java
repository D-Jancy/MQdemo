//package cool.jancy.mqdemo.rabbitmqtest;
//
//
//import com.rabbitmq.client.Channel;
//import org.springframework.amqp.core.Message;
//import org.springframework.amqp.rabbit.annotation.RabbitListener;
//import org.springframework.stereotype.Component;
//
//import java.io.IOException;
//
///**
// * @author dengjie
// * @version 1.0
// * @ClassName : DeadConsumer
// * @description: TODO
// * @date 2022/10/14 11:40
// */
//@Component
//public class DeadConsumer {
//
//    @RabbitListener(queues = "dead_queue")
//    public void listenMessage(Channel channel, Message message) throws IOException {
//        // 消息投递序号，消息每次投递该值都会+1
//        long deliveryTag = message.getMessageProperties().getDeliveryTag();
//        try {
//            System.out.println("死信队列成功接受到消息:" + message);
//            // 签收消息
//            /**
//             * 参数1：消息投递序号
//             * 参数2：是否一次可以签收多条消息
//             */
//            channel.basicAck(deliveryTag, false);
//        } catch (Exception e) {
//            System.out.println("消息消费失败！");
//            // 拒签消息
//            /**
//             * 参数1：消息投递序号
//             * 参数2：是否一次可以拒签多条消息
//             * 参数3：拒签后消息是否重回队列
//             */
//            channel.basicNack(deliveryTag, false, true);
//        }
//    }
//}
