package Messaging;

import com.ProActiveQueue.ProActiveQueueClient.Configuration.QueueConfig;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;

public class SynchMessageTest {

    private final Logger log = LoggerFactory.getLogger(SynchMessageTest.class);

    private static ActiveMQConnectionFactory factory;

    @BeforeAll
    static void initialize() throws JMSException {
        QueueConfig config = new QueueConfig("vm://localhost?broker.persistent=false",null,null);
        SynchMessageTest.factory = config.getActiveMQConnectionFactory();
    }


}
