package ai.quantumsense.tgmonitor.ipc.core;

import ai.quantumsense.tgmonitor.corefacade.CoreFacade;
import ai.quantumsense.tgmonitor.ipc.api.serializer.Serializer;
import ai.quantumsense.tgmonitor.ipc.api.serializer.pojo.EmptyResponse;
import ai.quantumsense.tgmonitor.ipc.api.serializer.pojo.Request;
import ai.quantumsense.tgmonitor.ipc.api.serializer.pojo.Response;
import ai.quantumsense.tgmonitor.ipc.api.serializer.pojo.ValueResponse;
import ai.quantumsense.tgmonitor.logincodeprompt.LoginCodePrompt;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static ai.quantumsense.tgmonitor.ipc.api.HeaderKeys.*;
import static ai.quantumsense.tgmonitor.ipc.api.Requests.*;
import static ai.quantumsense.tgmonitor.ipc.api.Queues.REQUEST_QUEUE;

public class RequestHandler {

    private Logger logger = LoggerFactory.getLogger(RequestHandler.class);

    private Connector connector;
    private Channel channel;
    private Serializer serializer = new Serializer();
    private CoreFacade coreFacade;

    private boolean isLoggingIn = false;

    public RequestHandler(String amqpUri, CoreFacade coreFacade) {
        connector = new Connector(amqpUri);
        channel = connector.getChannel();
        this.coreFacade = coreFacade;
        try {
            declareQueue();
            startConsumer();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void stop() {
        connector.disconnect();
    }

    private void declareQueue() throws IOException {
        logger.debug("Declaring queue " + REQUEST_QUEUE);
        channel.queueDeclare(REQUEST_QUEUE, false, false, true, null);
    }

    private void startConsumer() throws IOException {
        String consumerTag = channel.basicConsume(REQUEST_QUEUE, true, this::handleRequest, this::handleConsumerCancel);
        logger.debug("Starting to listen for requests on queue " + REQUEST_QUEUE + " with consumer " + consumerTag);
    }

    private void handleRequest(String consumerTag, Delivery delivery) throws IOException {
        if (isLoggingIn) {
            logger.debug("Receiving request while a login request is still being processed");
            sendErrorResponse("Login request of another UI instance already in process. Try later.", delivery.getProperties());
        }
        else {
            Request request = serializer.deserializeRequest(delivery.getBody());
            logger.debug("Receiving request " + request);
            if (isLoginRequest(request))
                handleLoginRequest(request, delivery.getProperties());
            else
                handleNormalRequest(request, delivery.getProperties());
        }
//        StringBuilder sb = new StringBuilder();
//        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
//        for (Thread t : threadSet) {
//            sb.append(t.getName() + " " + t.isAlive() + " " + t.getState());
//        }
//        logger.debug("All threads:\n" + sb.toString());
    }

    private void handleLoginRequest(Request request, AMQP.BasicProperties props) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                logger.debug("Handling login request in separate thread");
                isLoggingIn = true;
                String loginCodeRequestQueue = getLoginCodeRequestQueue(props);
                LoginCodePrompt loginCodePrompt = new LoginCodePrompt() {
                    @Override
                    public String promptLoginCode() {
                        LoginCodeRequestSender s = new LoginCodeRequestSender(loginCodeRequestQueue, channel);
                        return s.getLoginCode();
                    }
                };
                String phoneNumber = (String) request.getArgs().get(0);
                logger.debug("Calling CoreFacade login() method with phone number " + phoneNumber);
                coreFacade.login(phoneNumber, loginCodePrompt);
                logger.debug("Login succeeded");
                try {
                    RequestHandler.this.sendEmptyResponse(props);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                isLoggingIn = false;
            }
            private String getLoginCodeRequestQueue(AMQP.BasicProperties props) {
                if (props.getHeaders() == null || !props.getHeaders().containsKey(LOGIN_CODE_REQUEST_QUEUE))
                    throw new RuntimeException("Login request does not contain required login code request queue in header");
                return props.getHeaders().get(LOGIN_CODE_REQUEST_QUEUE).toString();
            }
        });
        thread.start();
        //logger.debug("Thread " + thread + ", isDaemon: " + thread.isDaemon() + ", isAlive: " + thread.isAlive());
    }

    @SuppressWarnings("unchecked")
    private void handleNormalRequest(Request request, AMQP.BasicProperties props) throws IOException {
        List<Object> args = request.getArgs();
        switch (request.getName()) {
            case LOGOUT:
                coreFacade.logout();
                sendEmptyResponse(props);
                break;
            case IS_LOGGED_IN:
                sendValueResponse(coreFacade.isLoggedIn(), props);
                break;
            case START:
                coreFacade.start();
                sendEmptyResponse(props);
                break;
            case STOP:
                coreFacade.start();
                sendEmptyResponse(props);
                break;
            case IS_RUNNING:
                sendValueResponse(coreFacade.isRunning(), props);
                break;
            case GET_PHONE_NUMBER:
                sendValueResponse(coreFacade.getPhoneNumber(), props);
                break;

            case GET_PEERS:
                sendValueResponse(coreFacade.getPeers(), props);
                break;
            case SET_PEERS:
                coreFacade.setPeers((Set<String>) args.get(0));
                break;
            case ADD_PEER:
                coreFacade.addPeer((String) args.get(0));
                break;
            case ADD_PEERS:
                coreFacade.addPeers((Set<String>) args.get(0));
                break;
            case REMOVE_PEER:
                coreFacade.removePeer((String) args.get(0));
                break;
            case REMOVE_PEERS:
                coreFacade.removePeers((Set<String>) args.get(0));
                break;

            case GET_PATTERNS:
                sendValueResponse(coreFacade.getPatterns(), props);
                break;
            case SET_PATTERNS:
                coreFacade.setPatterns((Set<String>) args.get(0));
                break;
            case ADD_PATTERN:
                coreFacade.addPattern((String) args.get(0));
                break;
            case ADD_PATTERNS:
                coreFacade.addPatterns((Set<String>) args.get(0));
                break;
            case REMOVE_PATTERN:
                coreFacade.removePattern((String) args.get(0));
                break;
            case REMOVE_PATTERNS:
                coreFacade.removePatterns((Set<String>) args.get(0));
                break;

            case GET_EMAILS:
                sendValueResponse(coreFacade.getEmails(), props);
                break;
            case SET_EMAILS:
                coreFacade.setEmails((Set<String>) args.get(0));
                break;
            case ADD_EMAIL:
                coreFacade.addEmail((String) args.get(0));
                break;
            case ADD_EMAILS:
                coreFacade.addEmails((Set<String>) args.get(0));
                break;
            case REMOVE_EMAIL:
                coreFacade.removeEmail((String) args.get(0));
                break;
            case REMOVE_EMAILS:
                coreFacade.removeEmails((Set<String>) args.get(0));
                break;

            default:
                throw new RuntimeException("Unknown request: " + request.getName());
        }
    }

    private void sendEmptyResponse(AMQP.BasicProperties requestProps) throws IOException {
        Response response = new EmptyResponse();
        sendResponse(response, requestProps);
    }

    private void sendValueResponse(Object value, AMQP.BasicProperties requestProps) throws IOException {
        Response response = new ValueResponse(value);
        sendResponse(response, requestProps);
    }

    private void sendResponse(Response response, AMQP.BasicProperties requestProps) throws IOException {
        logger.debug("Sending back response " + response + " with correlation ID " + requestProps.getCorrelationId() + " on queue " + requestProps.getReplyTo());
        byte[] body = serializer.serializeResponse(response);
        AMQP.BasicProperties responseProps = new AMQP.BasicProperties.Builder()
                .correlationId(requestProps.getCorrelationId())
                .build();
        channel.basicPublish("", requestProps.getReplyTo(), responseProps, body);
    }

    private void sendErrorResponse(String errorMsg, AMQP.BasicProperties requestProps) throws IOException {
        logger.debug("Sending back error response with message \"" + errorMsg + "\" with correlation ID " + requestProps.getCorrelationId() + " on queue " + requestProps.getReplyTo());
        Map<String, Object> header = new HashMap<>();
        header.put(ERROR, errorMsg);
        AMQP.BasicProperties responseProps = new AMQP.BasicProperties.Builder()
                .correlationId(requestProps.getCorrelationId())
                .headers(header)
                .build();
        channel.basicPublish("", requestProps.getReplyTo(), responseProps, null);
    }

    private boolean isLoginRequest(Request request) {
        return request.getName().equals(LOGIN);
    }

    private void handleConsumerCancel(String consumerTag) {
        logger.debug("Cancelling consumer " + consumerTag);
    }
}