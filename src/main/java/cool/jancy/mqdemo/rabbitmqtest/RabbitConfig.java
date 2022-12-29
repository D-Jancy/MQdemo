package cool.jancy.mqdemo.rabbitmqtest;

import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * @author dengjie
 * @version 1.0
 * @ClassName : RabbitConfig
 * @description: TODO
 * @date 2022/10/14 10:48
 */
@SpringBootConfiguration
public class RabbitConfig {

    private final String EXCHANGE_NAME = "normal_exchange";
    private final String QUEUE_NAME = "normal_queue";

    private final String DEAD_EXCHANGE = "dead_exchange";
    private final String DEAD_QUEUE = "dead_queue";

    @Bean(DEAD_EXCHANGE)
    public Exchange deadExchange() {
        return ExchangeBuilder.directExchange(DEAD_EXCHANGE)
                .durable(true)
                .build();
    }

    @Bean(DEAD_QUEUE)
    public Queue deadQueue() {
        return QueueBuilder.durable(DEAD_QUEUE)
                .build();
    }

    @Bean
    public Binding bindDead(@Qualifier(DEAD_EXCHANGE) Exchange exchange, @Qualifier(DEAD_QUEUE) Queue queue) {
        return BindingBuilder.bind(queue)
                .to(exchange)
                .with("dead")
                .noargs();
    }

    @Bean(EXCHANGE_NAME)
    public Exchange normalExchange() {
        return ExchangeBuilder.directExchange(EXCHANGE_NAME)
                .durable(true)
                .build();
    }

    @Bean(QUEUE_NAME)
    public Queue normalQueue() {
        return QueueBuilder.durable(QUEUE_NAME)
                .deadLetterExchange(DEAD_EXCHANGE)
                .deadLetterRoutingKey("dead")
                //设置队列的最大优先级，最大可以设置到255，官网推荐不要超过10，如果设置太高比较浪费资源
                //.maxPriority(10)
                //队列所有消息存活时间
                .ttl(10 * 1000)
                //队列最大长度 为10
                .maxLength(10)
                //队列中的所有消息的最大字节数总和限制
                //.maxLengthBytes(1024)
                //不满足以上限制的消息将发往死信交换机，如果没有设置死信队列也没有配置退回机制则到期后将移除队列，不过只有在队列顶端的过期消息才会被立即移除
                .build();
    }

    @Bean
    public Binding bindNormal(@Qualifier(EXCHANGE_NAME) Exchange exchange, @Qualifier(QUEUE_NAME) Queue queue) {
        return BindingBuilder.bind(queue)
                .to(exchange)
                .with("normal")
                .noargs();
    }
}
