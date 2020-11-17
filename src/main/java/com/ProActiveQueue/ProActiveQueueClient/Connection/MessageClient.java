package com.ProActiveQueue.ProActiveQueueClient.Connection;

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
 * <p>
 * This class is a mid-level abstraction over the activeMQ message library. It allows for Topic or Queue
 * connection, filtering using MessageProperties, and async or sync message operations.
 * </p>
 *
 * <ul>
 *     <li>
 *         <b>Low Level</b>: ActiveMQMessageConsumerImpl, ActiveMQMessageProducer
 *     </li>
 *     <li>
 *         <b>Mid Level</b>: MessageClient
 *     </li>
 *     <li>
 *        <b>High Level</b>: A Destination specific class that extends MessageClient and overrides methods
 *     </li>
 * </ul>
 *
 * <p>
 *     In addition to providing get and send message requests, you can also send batched messages or request a
 *     certain number of batched messages.
 * </p>
 *
 * <p>
 * As a reminder, if you do not extend MessageClient, do to generics, you will need to provide
 * the class of the message object that will be sent in the queue/topic.
 * </p>
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

    /**
     * This method can be called to close down the low level ActiveMQ consumer and producer
     * @throws JMSException
     */
    public void close() throws JMSException {
        this.messageConsumer.closeConsumers();
        this.messageProducer.close();
    }

    /**
     * <b>
     *     ASYNC Message Receiver
     * </b>
     * <p>
     *     This method allows you to register an Asynchronous message listener to this queue.
     * </p>
     *
     * @param onMessage a lambda function to consume messages as they become available
     */
    public void onMessageReceived(Consumer<P> onMessage) throws JMSException {
        messageConsumer.onMessageReceived(onMessage,clazz,null);
    }

    /**
     * <b>
     *     ASYNC Message Receiver
     * </b>
     * <p>
     * This method allows you to register an Asynchronous message listener to this queue. Additionally, you can provide
     * MessageProperties in order to filter the messages that would be received.
     * </p>
     *
     * @param onMessage a lambda function to consume messages as they become available
     * @param props an Object that holds message filters that are applied to the returned messages
     */
    public void onMessageReceived(Consumer<P> onMessage, MessageProperties props) throws JMSException {
        messageConsumer.onMessageReceived(onMessage,clazz,props);
    }

    /**
     * <b>
     *     SYNCHRONOUS Message Receiver
     * </b>
     * <p>
     *     This method allows you to synchronously request a single message, transactional safe. Plain and simple
     * </p>
     * <p>
     *     If your consumer method throws an error while processing, the message will be rolled back to the
     *     message broker for another consumer to use
     * </p>
     * @param onMessage a method that will act on a recieved message if there is one available
     * @throws IOException
     * @throws JMSException
     */
    public void getMessage(Consumer<P> onMessage) throws IOException, JMSException {
        getMessage(onMessage,null);
    }

    /**
     * <b>
     *     SYNCHRONOUS Message Receiver
     * </b>
     * <p>
     *     This method allows you to synchronously request a single message, based on one or more filtering options
     *     based on the provided MessageProperties object, in a transaction safe environment.
     * </p>
     * <p>
     *     If your consumer method throws an error while processing, the message will be rolled back to the
     *     message broker for another consumer to use
     * </p>
     * @param onMessage a method that will act on a recieved message if there is one available that fits the filter
     * @param props a <b>MessageProperties</b> object that holds one or more filters for the requested message
     * @throws IOException
     * @throws JMSException
     */
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
     * <p>
     *     If your consumer method throws an error while processing, the message will be rolled back to the
     *     message broker for another consumer to use
     * </p>
     * <p>
     *     <i>Use getMessages(onMessage, messageCount, props) to apply filters to the messages</i>
     * </p>
     *
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
     * <p>
     *     If your consumer method throws an error while processing, the message will be rolled back to the
     *     message broker for another consumer to use
     * </p>
     *
     * <p>
     * Additionally, this method allows you to add filters to the messages you are requesting. These filters are
     * identified in the MessageProperties class which should be accessed through the builder() method. Some available
     * selectors are as follows:
     * </p>
     * <ul>
     *     <li>
     *         <b>MATCHES</b> (make sure all messages have the same value for a given property)
     *     </li>
     *     <li>
     *         <b>EQUALS</b> (make sure all messages have a given value for a given property)
     *     </li>
     *     <li>
     *         <b>GREATER_THAN</b> (make sure all messages have a greater numeric value than given for a given property)
     *     </li>
     *     <li>
     *         <b>LESS_THAN</b> (make sure all messages have a lesser numeric value than given for a given property)
     *     </li>
     * </ul>
     *
     * @param onMessage A Consumer lambda function to run on the returned List of <P> objects
     * @param messageCount the number of messages to try to return. If the queue doesn't have that many messages, it returns
     *                     all the messages in the queue
     * @param props an Object that holds message filters that are applied to the returned messages
     */
    public void getMessages(Consumer<List<P>> onMessage, Integer messageCount, MessageProperties props) throws IOException, JMSException {
        messageConsumer.getMessages(onMessage,messageCount, clazz,props);
    }

    /**
     * <p>
     *     This method allows you to send a single message to the broker and ensure that it is received
     * </p>
     * <p>
     *     <i>If you want to add filterable message properties to the message, use send(message,props)</i>
     * </p>
     * @param message an object of type <b>{@literal <}P{@literal >}</b> to send
     * @throws JsonProcessingException
     * @throws JMSException
     */
    public void send(P message) throws JsonProcessingException, JMSException {
        send(message, null);
    }

    /**
     * <p>
     *     This method allows you to send a single message to the broker and ensure that it is received. Additionally,
     *     it allows you to attach properties to the message that can be filtered on later by a consumer
     * </p>
     * @param message an object of type <b>{@literal <}P{@literal >}</b> to send
     * @param props a Properties object that contains filterable information about the message.
     * @throws JsonProcessingException
     * @throws JMSException
     */
    public void send(P message, Properties props) throws JsonProcessingException, JMSException {
        if(props != null){
            messageProducer.send(message, props);
        }else{
            messageProducer.send(message);
        }
        messageProducer.commit();
    }

    /**
     * <p>
     *     This method allows you to send a list of messages to the broker and ensure that they all are received
     * </p>
     * <p>
     *     <i>If you want to add filterable message properties to the messages, use sendAll(message,getProperty)</i>
     * </p>
     * @param messages a list of Objects of type <b>{@literal <}P{@literal >}</b> to send
     * @throws JsonProcessingException
     * @throws JMSException
     */
    public void sendAll(List<P> messages) throws JsonProcessingException, JMSException {
        sendAll(messages,null);
    }

    /**
     * <p>
     *     This method allows you to send a list of messages to the broker and ensure that they all are received.
     *     Additionally, it allows you to attach properties to these message objects that can be filtered on
     *     later by a consumer. Because a list of messages is being provided, you must provide a Function
     *     that can be used to get the property for each message object.
     * </p>
     * <p>
     *     The <b>getProperty</b> method is typically provided as a lambda function that takes the message
     *     object as input and outputs a Properties object with any desired properties attached.
     * </p>
     * <p>
     *     <b>Example:</b>
     *     <ul>
     *         <li>
     *             message has a method <i>getScanType()</i> that returns a string
     *         </li>
     *         <li>
     *             NmapScanType.class.getSimpleName() is the name of the property that will be filtered on later
     *         </li>
     *     </ul>
     * </p>
     * <p></p>
     * <pre>
     * (message) -> {
     *      if(message.getScanType() != null){
     *          Properties props = new Properties();
     *          props.setProperty(NmapScanType.class.getSimpleName(),
     *              message.getScanType().toString());
     *          return props;
     *      }else{
     *          return null;
     *      }
     *  }
     * </pre>
     * <p></p>
     * <p>If desired, you can add as many Properties for that messsage as you would like</p>
     *
     * @param messages a list of Objects of type <b>{@literal <}P{@literal >}</b> to send
     * @param getProperty a Funcion that takes a message object and returns a Properties object. See above for Example
     * @throws JsonProcessingException
     * @throws JMSException
     */
    public void sendAll(List<P> messages, Function<P, Properties> getProperty) throws JsonProcessingException, JMSException {
        for(P message : messages){
            Properties props = Optional.ofNullable(getProperty).map(getProp -> getProp.apply(message)).orElse(null);
            if(props != null){
                messageProducer.send(message, props);
            }else{
                messageProducer.send(message);
            }
        }
        messageProducer.commit();
    }
}
