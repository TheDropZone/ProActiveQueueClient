package Connection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This class is a mid-level abstraction over the activeMQ message library. It allows for Topic or Queue
 * connection, filtering using MessageProperties, and async or sync message operations.
 *
 * Low Level: ActiveMQMessageConsumerImpl, ActiveMQMessageProducer
 * Mid Level: MessageClient
 * High Level: A Destination specific class that extends MessageClient and overrides methods
 *
 * As a reminder, if you do not extend MessageClient, do to generics, you will need to provide
 * the class of the message object that will be sent in the queue/topic.
 *
 *
 * @param <P> the class of the message object that will be sent in the queue/topic.
 */
public class MessageClient<P> {

    private ActiveMQMessageConsumerImpl messageConsumer;

    private ActiveMQMessageProducer messageProducer;

    private Class<P> clazz;

    private ObjectMapper mapper;

    public MessageClient(){}

    /**
     * This constructor is used when the MessageClient class is extended for a specific Queue/Topic class. If
     * the MessageClient is extended, we don't need to pass in the Class of the message object
     * @param destination A Queue or a Topic for the messaging
     * @param factory the ActiveMQConnectionFactor to connect to the activeMQ server
     * @throws JMSException
     */
    public MessageClient(Destination destination, ActiveMQConnectionFactory factory) throws JMSException {
        this.messageConsumer = new ActiveMQMessageConsumerImpl(factory);
        this.messageProducer = new ActiveMQMessageProducer(factory);
        messageConsumer.setup(destination);
        messageProducer.setup(true,destination);
        mapper = new ObjectMapper();
        this.clazz = ((Class<P>) ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0]);
        if(this.clazz == null){
            throw new JMSException("Did you extend this class when using this constructor? This constructor " +
                    "was unable to determine what class is being used for messaging. Please use the alternative " +
                    "constructor so that you can provide the Message Class.");
        }
    }

    /**
     * This constructor is used when the MessageClient class is not extended and simply used on its own. Because of
     * generics in java, you have to provide the class of the object that you wish to send back and forth via ActiveMQ.
     *
     * @param destination A Queue or a Topic for the messaging
     * @param factory the ActiveMQConnectionFactor to connect to the activeMQ server
     * @param clazz the Class of the object that will be messaged
     * @throws JMSException
     */
    public MessageClient(Destination destination, ActiveMQConnectionFactory factory, Class<P> clazz) throws JMSException {
        this.messageConsumer = new ActiveMQMessageConsumerImpl(factory);
        this.messageProducer = new ActiveMQMessageProducer(factory);
        messageConsumer.setup(destination);
        messageProducer.setup(true,destination);
        mapper = new ObjectMapper();
        this.clazz = clazz;
    }

    public void close() throws JMSException {
        this.messageConsumer.closeConsumers();
        this.messageProducer.close();
    }

    /**
     * ASYNC Message Receiver
     * This method allows you to register an Asynchronous message listener to this queue.
     *
     * @param onMessage a lambda function to consume messages as they become available
     */
    public void onMessageReceived(Consumer<P> onMessage) throws JMSException {
        messageConsumer.onMessageReceived(onMessage,clazz,null);
    }

    /**
     * ASYNC Message Receiver
     * This method allows you to register an Asynchronous message listener to this queue. Additionally, you can provide
     * MessageProperties in order to filter the messages that would be received
     *
     * @param onMessage a lambda function to consume messages as they become available
     * @param props an Object that holds message filters that are applied to the returned messages
     */
    public void onMessageReceived(Consumer<P> onMessage, MessageProperties props) throws JMSException {
        messageConsumer.onMessageReceived(onMessage,clazz,props);
    }

    public void getMessage(Consumer<P> onMessage) throws IOException, JMSException {
        getMessage(onMessage,null);
    }

    public void getMessage(Consumer<P> onMessage, MessageProperties props) throws IOException, JMSException {
        messageConsumer.getMessages((onMessages) -> {
            try{
                if(!onMessages.isEmpty()){
                    onMessage.accept(onMessages.get(1));
                }else{
                    onMessage.accept(null);
                }
            }catch(Exception e){
                throw new RuntimeException(e);
            }
        },1,clazz,props);
    }

    /**
     * This method allows you to get a transactional bound list of messages, equivalent to messageCount in length, if available.
     * If that amount is not available, it will return a smaller amount, according to availability.
     * The method handles the acknowledgment and commit/rollback of the transaction in case things go bad.
     *
     * Use getMessages(onMessage, messageCount, props) to apply filters to the messages
     * @param onMessage A Consumer lambda function to run on the returned List of <P> objects
     * @param messageCount the number of messages to try to return. If the queue doesn't have that many messages, it returns
     *                     all the messages in the queue
     */
    public void getMessages(Consumer<List<P>> onMessage, Integer messageCount) throws IOException, JMSException {
        getMessages(onMessage,messageCount,null);
    }

    /**
     * This method allows you to get a transactional bound list of messages, equivalent to messageCount in length, if available.
     * If that amount is not available, it will return a smaller amount, according to availability.
     * The method handles the acknowledgment and commit/rollback of the transaction in case things go bad.
     *
     * Additionally, this method allows you to add filters to the messages you are requesting. These filters are
     * identified in the MessageProperties class which should be accessed through the builder() method. Some available
     * selectors are as follows:
     * MATCHES (make sure all messages have the same value for a given property)
     * EQUALS (make sure all messages have a given value for a given property)
     * GREATER_THAN (make sure all messages have a greater numeric value than given for a given property)
     * LESS_THAN (make sure all messages have a lesser numeric value than given for a given property)
     *
     * Use getMessages(onMessage, messageCount, props) to apply filters to the messages
     * @param onMessage A Consumer lambda function to run on the returned List of <P> objects
     * @param messageCount the number of messages to try to return. If the queue doesn't have that many messages, it returns
     *                     all the messages in the queue
     * @param props an Object that holds message filters that are applied to the returned messages
     */
    public void getMessages(Consumer<List<P>> onMessage, Integer messageCount, MessageProperties props) throws IOException, JMSException {
        messageConsumer.getMessages(onMessage,messageCount, clazz,props);
    }

    public void send(P host) throws JsonProcessingException, JMSException {
        send(host, null);
    }

    public void send(P host, Properties props) throws JsonProcessingException, JMSException {
        if(props != null){
            messageProducer.send(host, props);
        }else{
            messageProducer.send(host);
        }
        messageProducer.commit();
    }

    public void sendAll(List<P> hosts) throws JsonProcessingException, JMSException {
        sendAll(hosts,null);
    }

    public void sendAll(List<P> hosts, Function<P, Properties> getProperty) throws JsonProcessingException, JMSException {
        for(P host : hosts){
            Properties props = Optional.ofNullable(getProperty).map(getProp -> getProp.apply(host)).orElse(null);
            if(props != null){
                messageProducer.send(host, props);
            }else{
                messageProducer.send(host);
            }
        }
        messageProducer.commit();
    }
}
