package ai.quantumsense.tgmonitor.ipc.core;

import ai.quantumsense.tgmonitor.ipc.api.serializer.Serializer;
import ai.quantumsense.tgmonitor.ipc.api.serializer.pojo.Request;
import ai.quantumsense.tgmonitor.ipc.api.serializer.pojo.Response;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static ai.quantumsense.tgmonitor.ipc.api.Requests.GET_LOGIN_CODE;

class LoginCodeRequestSender {

    private Logger logger = LoggerFactory.getLogger(LoginCodeRequestSender.class);

    private String requestQueue;
    private String responseQueue = "login_code_response-" + makeId();
    private Channel channel;
    private Serializer serializer = new Serializer();

    private final BlockingQueue<String> responseHolder = new ArrayBlockingQueue<>(1);

    LoginCodeRequestSender(String requestQueue, Channel channel) {
        this.requestQueue = requestQueue;
        this.channel = channel;
    }

    String getLoginCode() {
        String loginCode = null;
        try {
            declareResponseQueue();
            sendRequest(new Request(GET_LOGIN_CODE));
            startResponseConsumer();
            loginCode = waitForResponse();
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return loginCode;
    }

    private void declareResponseQueue() throws IOException {
        logger.debug("Declaring login code response queue " + responseQueue);
        channel.queueDeclare(responseQueue, false, true, true, null);
    }

    private void sendRequest(Request request) throws IOException {
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .replyTo(responseQueue)
                .build();
        byte[] body = serializer.serializeRequest(request);
        logger.debug("Sending login code request on queue " + requestQueue);
        channel.basicPublish("", requestQueue, props, body);
    }

    private void startResponseConsumer() throws IOException {
        String consumerTag = channel.basicConsume(responseQueue, true, this::handleResponse, this::handleConsumerCancel);
        logger.debug("Starting listening for login code response on queue " + responseQueue + " with consumer " + consumerTag);
    }

    private void handleResponse(String consumerTag, Delivery delivery) throws IOException {
        Response response = serializer.deserializeResponse(delivery.getBody());
        String loginCode = (String) response.getValue();
        logger.debug("Received login code " + loginCode);
        responseHolder.offer(loginCode);
        channel.basicCancel(consumerTag);
    }

    private void handleConsumerCancel(String consumerTag) {
        logger.debug("Cancelling consumer " + consumerTag);
    }

    private String waitForResponse() throws InterruptedException {
        return responseHolder.take();
    }

    private String makeId() {
        return UUID.randomUUID().toString();
    }
}
