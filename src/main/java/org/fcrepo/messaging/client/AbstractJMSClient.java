package org.fcrepo.messaging.client;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractJMSClient {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJMSClient.class);                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
    private final String brokerUrl;                                           
    private final String topicName;
    private final MessageConsumer consumer;
    
    public AbstractJMSClient(String brokerUrl, String topicName) throws JMSException{
        this.brokerUrl = brokerUrl;
        this.topicName = topicName;
        ConnectionFactory jmsFac = new ActiveMQConnectionFactory(brokerUrl);
        Connection conn = jmsFac.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = sess.createTopic(topicName);
        consumer = sess.createConsumer(topic);
    }
    
    protected Message getNextMessage() throws JMSException {
        return consumer.receive();
    }
}
