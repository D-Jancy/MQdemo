package cool.jancy.mqdemo.rocketmqtest.batch;

import org.apache.rocketmq.common.message.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author dengjie
 * @version 1.0
 * @ClassName : MessageListSplitter
 * @description: TODO
 * // 消息列表分割器：其只会处理每条消息的大小不超4M的情况。
 * // 若存在某条消息，其本身大小大于4M，这个分割器无法处理，
 * // 其直接将这条消息构成一个子列表返回。并没有再进行分割
 * @date 2022/10/26 16:29
 */
public class MessageListSplitter implements Iterator<List<Message>> {

    // 指定极限值为4M
    private final int SIZE_LIMIT = 4 * 1024 * 1024;
    // 存放所有要发送的消息
    private final List<Message> messages;
    // 要进行批量发送消息的小集合起始索引
    private int currIndex;

    public MessageListSplitter(List<Message> messages) {
        this.messages = messages;
    }


    @Override
    public boolean hasNext() {
        // 判断当前开始遍历的消息索引要小于消息总数
        return currIndex < messages.size();
    }

    @Override
    public List<Message> next() {
        int nextIndex = currIndex;

        // 记录当前要发送的这一小批次消息列表的大小
        int totalSize = 0;
        for (; nextIndex < messages.size(); nextIndex++) {
            Message message = messages.get(nextIndex);
            /*
             *计算当前消息的大小
             */
            //1. 获取topic和body长度
            int tempSize = message.getTopic().length() + message.getBody().length;
            //2. 获取配置信息长度
            Map<String, String> properties = message.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                tempSize += entry.getKey().length() + entry.getValue().length();
            }
            //3. 加上log长度（20字节）
            tempSize += 20;

            //判断当前消息是否大于4M
            if (tempSize > SIZE_LIMIT) {
                //前进
                if (nextIndex - currIndex == 0) {
                    nextIndex++;
                }
                break;
            }

            //判断总消息体是否大于4M
            if (tempSize + totalSize > SIZE_LIMIT) {
                break;
            } else {
                //携带当前消息
                totalSize += tempSize;
            }
        }

        // 获取当前messages列表的子集合[currIndex, nextIndex)
        List<Message> subList = messages.subList(currIndex, nextIndex);
        // 下次遍历的开始索引
        currIndex = nextIndex;
        return subList;
    }
}
