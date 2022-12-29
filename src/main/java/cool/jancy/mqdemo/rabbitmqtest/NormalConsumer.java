//package cool.jancy.mqdemo.rabbitmqtest;
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
// * @ClassName : Consumer
// * @description: TODO
// * @date 2022/10/14 11:24
// */
//@Component
//public class NormalConsumer {
//
//    @RabbitListener(queues = "normal_queue")
//    public void listenMessage(Channel channel, Message message) throws IOException, InterruptedException {
//        // 消息投递序号，消息每次投递该值都会+1
//        long deliveryTag = message.getMessageProperties().getDeliveryTag();
//        try {
//            //1.模拟异常产生超时发送至死信交换机
//            int i = 1 / 0;
//            System.out.println("成功接受到消息:" + message);
//            // 签收消息
//            /**
//             * 参数1：消息投递序号
//             * 参数2：是否一次可以签收多条消息
//             */
//            channel.basicAck(deliveryTag, false);
//
//            //2.直接拒签发送至死信交换机
//            //channel.basicNack(deliveryTag, false, false);
//        } catch (Exception e) {
//            Thread.sleep(2000);
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
