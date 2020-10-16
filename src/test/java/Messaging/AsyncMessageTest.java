package Messaging;

import Configuration.QueueConfig;
import Connection.MessageClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class AsyncMessageTest {

    private final Logger log = LoggerFactory.getLogger(AsyncMessageTest.class);

    private static ActiveMQConnectionFactory factory;


    @BeforeAll
    static void initialize() throws JMSException {
        QueueConfig config = new QueueConfig("vm://localhost?broker.persistent=false",null,null);
        AsyncMessageTest.factory = config.getActiveMQConnectionFactory();
    }

    @Test
    void asyncQueue() throws JsonProcessingException, JMSException, InterruptedException {
        AtomicReference requestReference = new AtomicReference<TestObject>(null);
        TestMessageClient queueClient = new TestMessageClient(QueueConfig.getQueueWithName("testQueue"), AsyncMessageTest.factory);

        queueClient.onMessageReceived(hostRequest -> {
            log.info("Received host request on async listener");
            requestReference.set(hostRequest);
        });
        log.info("Registered Async message listener");

        log.info("Sending Request");
        queueClient.send(new TestObject("myId","Hello World!"));
        log.info("Request Sent");

        Thread.sleep(1000);
        Assertions.assertNotNull(requestReference.get());
        System.out.println(requestReference.get());
        queueClient.close();
    }

    @Test
    void asyncTransactionalQueue() throws JsonProcessingException, JMSException, InterruptedException {
        AtomicReference requestReference = new AtomicReference<TestObject>(null);
        TestMessageClient queueClient1 = new TestMessageClient(QueueConfig.getQueueWithName("testQueue"), AsyncMessageTest.factory);
        TestMessageClient queueClient2 = new TestMessageClient(QueueConfig.getQueueWithName("testQueue"), AsyncMessageTest.factory);
        TestMessageClient queueClient3 = new TestMessageClient(QueueConfig.getQueueWithName("testQueue"), AsyncMessageTest.factory);

        queueClient1.onMessageReceived(hostRequest -> {
            log.info("Received messaged request on async listener. Throwing error");
            int i = 10/0;
            requestReference.set(hostRequest);
        });
        log.info("Registered Async message listener 1");

        queueClient2.onMessageReceived(hostRequest -> {
            log.info("Received host request on 2nd async listener");
            requestReference.set(hostRequest);
        });
        log.info("Registered Async message listener 2");

        log.info("Sending Request");
        queueClient3.send(new TestObject("myId","Hello World!"));
        log.info("Request Sent");

        Thread.sleep(100);
        Assertions.assertNotNull(requestReference.get());
        System.out.println(requestReference.get().toString());
        queueClient1.close();
        queueClient2.close();
        queueClient3.close();
    }

    @Test
    void asyncTopic() throws JMSException, InterruptedException, JsonProcessingException {
        AtomicInteger atomicCount = new AtomicInteger(0);

        int count = 10;
        List<Thread> joins = new ArrayList<>();
        for(Integer i = 0; i < count; i++){
            final int index = i;
            Thread thread = (new Thread(() -> {
                try {
                    TestMessageClient topicClient = new TestMessageClient(QueueConfig.getTopicWithName("testTopic"), AsyncMessageTest.factory);
                    topicClient.onMessageReceived((message -> {
                        System.out.println(message);
                        atomicCount.incrementAndGet();
                        try {
                            topicClient.close();
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }));
                    System.out.println("Registered listener " + index);
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }));
            thread.start();
            joins.add(thread);
        }
        for (Thread thread : joins) {
            thread.join();
        }

        TestMessageClient topicClient = new TestMessageClient(QueueConfig.getTopicWithName("testTopic"), AsyncMessageTest.factory);
        topicClient.send(new TestObject("myId","Hello World!"));
        Thread.sleep(100);
        Assertions.assertEquals(count,atomicCount.get());

    }

    public class TestMessageClient extends MessageClient<TestObject>{

        private Integer batchSize = 2;

        public TestMessageClient(Destination destination, ActiveMQConnectionFactory factory) throws JMSException {
            super(destination,factory);
        }

        public void getMessages (Consumer<List<TestObject>> onMessage) throws IOException, JMSException {
            super.getMessages(onMessage, batchSize);
        }

        @Override
        public void sendAll(List<TestObject> messages) throws JsonProcessingException, JMSException {
            super.sendAll(messages,(host) -> {
               return null;
            });
        }

        @Override
        public void send(TestObject message) throws JsonProcessingException, JMSException {
            super.send(message, null);
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class TestObject{
        private String id;
        private String message;
    }
}
