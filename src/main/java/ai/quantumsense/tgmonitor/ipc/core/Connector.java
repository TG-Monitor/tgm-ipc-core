package ai.quantumsense.tgmonitor.ipc.core;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

class Connector {

    private Logger logger = LoggerFactory.getLogger(Connector.class);

    private Connection connection;
    private Channel channel;
    private String amqpUri;


    Connector(String amqpUri) {
        this.amqpUri = amqpUri;
        connect();
    }

    private void connect() {
        logger.debug("Establishing connection to RabbitMQ server on " + amqpUri);
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(amqpUri);
            connection = factory.newConnection();
            channel = connection.createChannel();
        } catch (IOException | TimeoutException | KeyManagementException
                | NoSuchAlgorithmException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    void disconnect() {
        logger.debug("Closing connection to RabbitMQ server on " + amqpUri);
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Channel getChannel() {
        return channel;
    }
}
