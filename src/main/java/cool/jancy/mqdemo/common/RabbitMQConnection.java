package cool.jancy.mqdemo.common;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author dengjie
 * @version 1.0
 * @ClassName : RabbitMQConnection
 * @description: TODO
 * @date 2022/10/13 16:29
 */
public class RabbitMQConnection {


    public static Connection getConnection() throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("yourIp");
        factory.setUsername("yourUser");
        factory.setPassword("yourPwd");
        factory.setPort(5672);
        factory.setVirtualHost("/");
        return factory.newConnection();

    }
}
