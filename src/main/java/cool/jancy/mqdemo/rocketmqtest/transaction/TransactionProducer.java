package cool.jancy.mqdemo.rocketmqtest.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author dengjie
 * @version 1.0
 * @ClassName : TransactionProducer
 * @description: TODO
 * @date 2022/10/26 14:31
 */
public class TransactionProducer {

    public static void main(String[] args) {

        TransactionMQProducer producer = new TransactionMQProducer("producer-grp4");
        producer.setNamesrvAddr("yourIp:9876");

        //定义事务监听器
        TransactionListener listener = new TransactionListener() {

            // 回调操作方法
            // 消息预提交成功就会触发该方法的执行，用于完成本地事务
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                System.out.println("预提交消息成功：" + msg);
                // 假设的事务成功与否的判断逻辑
                // 假设接收到TAGA的消息就表示扣款操作成功，TAGB的消息表示扣款失败，TAGC表示扣款结果不清楚，需要执行消息回查
                if (StringUtils.equals("TAGA", msg.getTags())) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (StringUtils.equals("TAGB", msg.getTags())) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else if (StringUtils.equals("TAGC", msg.getTags())) {
                    return LocalTransactionState.UNKNOW;
                }
                return LocalTransactionState.UNKNOW;
            }

            // 消息回查方法
            // 引发消息回查的原因最常见的有两个：
            // 1)回调操作返回UNKNWON
            // 2)TC没有接收到TM的最终全局事务确认指令
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println("执行消息回查" + msg.getTags());
                //按照回调操作方法逻辑思路修改进行回查，但demo为了便捷体现事务能回调直接返回成功
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        };

        //定义生产者线程池
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        try {
            producer.start();
            producer.setTransactionListener(listener);
            producer.setExecutorService(threadPoolExecutor);

            String[] tags = {"TAGA", "TAGB", "TAGC"};
            for (int i = 0; i < 3; i++) {
                byte[] body = ("Hi," + i).getBytes();
                Message msg = new Message("transact-topic", tags[i], body);
                // 发送事务消息
                // 第二个参数用于指定在执行本地事务时要使用的业务参数，即executeLocalTransaction()中的object参数
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.println("发送结果为：" + sendResult.getSendStatus());
            }

            //需要回查所以不能停止producer
            //producer.shutdown();
            //System.out.println("producer shutdown");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
