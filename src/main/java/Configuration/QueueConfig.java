package Configuration;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.Queue;
import javax.jms.Topic;

public class QueueConfig {

    private String brokerUrl;
    private String username;
    private String password;

    public QueueConfig(String brokerUrl, String username, String password){
        this.brokerUrl = brokerUrl;
        this.username = username;
        this.password = password;
    }

    public ActiveMQConnectionFactory getActiveMQConnectionFactory() {
        return getActiveMQConnectionFactory(null);
    }

    public ActiveMQConnectionFactory getActiveMQConnectionFactory(ActiveMQPrefetchPolicy prefetchPolicy) {
        ActiveMQConnectionFactory activeMQConnectionFactory =
                new ActiveMQConnectionFactory();
        if(prefetchPolicy == null){
            prefetchPolicy = new ActiveMQPrefetchPolicy();
            prefetchPolicy.setQueuePrefetch(0);
            prefetchPolicy.setTopicPrefetch(0);
            prefetchPolicy.setQueueBrowserPrefetch(0);
            activeMQConnectionFactory.setPrefetchPolicy(prefetchPolicy);
        }else{
            activeMQConnectionFactory.setPrefetchPolicy(prefetchPolicy);
        }
        activeMQConnectionFactory.setBrokerURL(brokerUrl);
        activeMQConnectionFactory.setMaxThreadPoolSize(1);
        if(username != null){
            activeMQConnectionFactory.setUserName(username);
        }
        if(password != null){
            activeMQConnectionFactory.setPassword(password);
        }

        return activeMQConnectionFactory;
    }


    public static Queue getQueueWithName(String queueName){
        return new ActiveMQQueue(queueName);
    }

    public static Topic getTopicWithName(String topicName){
        return new ActiveMQTopic(topicName);
    }

}
