package com.ProActiveQueue.ProActiveQueueClient.Connection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ActiveMQMessageConsumerImpl {

    private final Logger log = LoggerFactory.getLogger(ActiveMQMessageConsumerImpl.class);

    private Destination destination;
    private ObjectMapper mapper;
    private ActiveMQMessageConsumer consumer;
    private Connection asyncConnection;
    private Connection syncConnection;
    private ActiveMQConnectionFactory factory;
    private Integer synchronousPrefetch = 0;
    private Integer asynchronousPrefetch = 10;

    public ActiveMQMessageConsumerImpl(ActiveMQConnectionFactory factory) {
        this.factory = factory;
        this.mapper = new ObjectMapper();
    }

    public void setup(Destination destination) throws JMSException {
        this.destination = destination;
        syncConnection = newConnection(false);
        asyncConnection = newConnection(true);
    }

    public void setup(Destination destination, Integer synchronousPrefetch, Integer asynchronousPrefetch) throws JMSException {
        this.destination = destination;
        this.synchronousPrefetch = synchronousPrefetch;
        this.asynchronousPrefetch = asynchronousPrefetch;
        syncConnection = newConnection(false);
        asyncConnection = newConnection(true);
    }

    /**
     * Once an error occurs during onMessageReceived async message processing, the session is rolled back and closed so
     * that other consumers can have the change to process that message.
     *
     * That also means that the current registered async message listener is no longer operating since the session was closed.
     * If you would like to do something, like register another listener, close down the app, etc, you can add an error
     * handler via this method to execute code on async message listener errors.
     */
    public void onMessageReceivedErrorHandler(Consumer<Exception> onError){
        //TODO Implement async error handling since async listener closes session on error
    }

    /**
     * Closes the async consumers down, so that they are no longer listening for messages. This does not close the
     * com.ProActiveQueue.ProActiveQueueClient.Connection objects in this class.
     * @throws JMSException
     */
    public void closeConsumers() throws JMSException {
        if(consumer != null){
            consumer.close();
        }
    }

    /**
     * ASYNC Message Receive
     * @param onMessage
     * @param clazz
     * @param properties
     * @param <T>
     */
    public <T> void onMessageReceived(Consumer<T> onMessage, Class<T> clazz, MessageProperties properties) throws JMSException {
        Session session = getSession(true, Session.CLIENT_ACKNOWLEDGE, true);

        String filter = Optional.ofNullable(properties)
                .map(props -> props.getMessageProperties(false)
                        .stream()
                        .map(prop -> prop.getFilter())
                        .collect(Collectors.joining(",")))
                .orElse(null);
        if(filter != null && !filter.equals("")){
            consumer = (ActiveMQMessageConsumer) session.createConsumer(destination, filter);
        }else{
            consumer = (ActiveMQMessageConsumer) session.createConsumer(destination);
        }

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
               try{
                   T messageObj = mapper.readValue(((TextMessage) message).getText(), clazz);
                   try{
                       message.acknowledge();
                       onMessage.accept(messageObj);
                       session.commit();
                   }catch(Exception e){
                       log.error("Rolling back transaction due to processing error",e);
                       session.rollback();
                       session.close();
                   }
               } catch (JsonMappingException e) {
                   e.printStackTrace();
               } catch (JsonProcessingException e) {
                   e.printStackTrace();
               } catch (JMSException | IOException e) {
                   e.printStackTrace();
               }
            }
        });
    }

    /**
     * SYNCHRONOUS Message Receive
     * @param onMessage
     * @param numberOfMessages
     * @param clazz
     * @param props
     * @param <T>
     * @throws JMSException
     * @throws JsonProcessingException
     */
    public <T> void getMessages(Consumer<List<T>> onMessage, Integer numberOfMessages, Class<T> clazz, MessageProperties props) throws JMSException, IOException {
        Session session = getSession(true,Session.CLIENT_ACKNOWLEDGE, false);
        List<T> messageQueue = new ArrayList<T>();

        String filter = props.getMessageProperties(false).stream().map(prop -> prop.getFilter()).collect(Collectors.joining(","));
        if(!filter.equals("")){
            consumer = (ActiveMQMessageConsumer) session.createConsumer(destination, filter);
        }else{
            consumer = (ActiveMQMessageConsumer) session.createConsumer(destination);
        }

        Message message = consumer.receive(1000 * 30);

        if(message != null){
            T messageObj = mapper.readValue(((TextMessage) message).getText(), clazz);
            message.acknowledge();
            messageQueue.add(messageObj);
            log.info("Received a message from " + destination.toString());

            final Message messageFinal = message;
            String matchFilters = props.getMessageProperties(true).stream()
                .map(property -> {
                    try {
                        Object value = messageFinal.getObjectProperty(property.getPropertyName());
                        return property.getInputBasedFilter(value);
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .filter(matchFilter -> matchFilter != null)
                .collect(Collectors.joining(","));


            if(!matchFilters.equals("")){
                String filterString = ((filter.equals("")) ? "" : (filter + ",")) + matchFilters;
                consumer = (ActiveMQMessageConsumer) session.createConsumer(destination,filterString);
                log.info("Grabbing all requests with filter: " + filterString);
            }

            while(messageQueue.size() <= numberOfMessages && message != null){
                message = consumer.receive(10);
                if(message != null){

                    messageQueue.add((T)mapper.readValue(((TextMessage)message).getText(),clazz));
                    message.acknowledge();
                    log.info("Received " + messageQueue.size() + " messages from " + destination.toString());
                }
            }
            try{
                onMessage.accept(messageQueue);
                session.commit();
            }catch(Exception e){
                log.error("Rolling back transaction due to processing error",e);
                session.rollback();
            }
        }
        session.close();
    }

    private Session getSession(boolean transacted, int acknowledgeMode, boolean async) throws JMSException {
        Connection connection = async ? asyncConnection : syncConnection;
        try{
            return connection.createSession(transacted, acknowledgeMode);
        }catch(JMSException e){
            connection.close();
            connection = newConnection(async);
            if(async){
                asyncConnection = connection;
            }else{
                syncConnection = connection;
            }
            return connection.createSession(transacted, acknowledgeMode);
        }
    }

    private Connection newConnection(boolean async) throws JMSException {
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setQueuePrefetch(async ? asynchronousPrefetch : synchronousPrefetch);
        prefetchPolicy.setTopicPrefetch(async ? asynchronousPrefetch : synchronousPrefetch);
        prefetchPolicy.setQueueBrowserPrefetch(async ? asynchronousPrefetch : synchronousPrefetch);
        factory.setPrefetchPolicy(prefetchPolicy);
        if(async){
            Connection connection = factory.createConnection();
            connection.start();
            return connection;
        }else{
            Connection connection = factory.createConnection();
            connection.start();
            return connection;
        }
    }
}
