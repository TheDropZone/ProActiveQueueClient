package com.ProActiveQueue.ProActiveQueueClient.Connection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Map;
import java.util.Properties;

public class ActiveMQMessageProducer {
    private ActiveMQConnectionFactory connFactory;

    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageProducer msgProducer;
    private ObjectMapper mapper;
    private boolean transacted;

    public ActiveMQMessageProducer(ActiveMQConnectionFactory factory) {
        this.connFactory = factory;
        this.mapper = new ObjectMapper();
    }

    public void setup(boolean transacted, Destination destination)
            throws JMSException {
        this.transacted = transacted;
        setConnection();
        setSession(transacted);
        this.destination = destination;
        setMsgProducer();
    }

    public void close() throws JMSException {
        if (msgProducer != null) {
            msgProducer.close();
            msgProducer = null;
        }

        if (session != null) {
            session.close();
            session = null;
        }
        if (connection != null) {
            connection.close();
            connection = null;
        }

    }

    public void commit() throws JMSException {
        if (transacted) {
            session.commit();
        }
    }

    public void send(Object message) throws JMSException, JsonProcessingException {
        String stringMessage = mapper.writeValueAsString(message);
        TextMessage textMessage = session.createTextMessage(stringMessage);
        msgProducer.send(destination, textMessage);
        // msgProducer.send(textMessage, DeliveryMode.NON_PERSISTENT, 0, 0);

    }

    public void send(Object message, Properties properties) throws JMSException, JsonProcessingException {
        String stringMessage = mapper.writeValueAsString(message);
        TextMessage textMessage = session.createTextMessage(stringMessage);
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            textMessage.setStringProperty((String) entry.getKey(), (String) entry.getValue());
        }
        msgProducer.send(destination, textMessage);
    }

    private void setMsgProducer() throws JMSException {
        msgProducer = session.createProducer(destination);

    }

    private void setSession(final boolean transacted) throws JMSException {
        // transacted=true for better performance to push message in batch mode
        session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
    }

    private void setConnection() throws JMSException {
        connection = connFactory.createConnection();
        connection.start();
    }

}