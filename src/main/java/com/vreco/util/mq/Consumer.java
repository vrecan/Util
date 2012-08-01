package com.vreco.util.mq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Basic connection library to connect to AMQ and consume messages.
 * @author Ben Aldrich
 */
public class Consumer implements AutoCloseable {

  private MessageConsumer consumer;
  private Connection connection;
  private Session session;
  private Destination destination;
  private boolean tempQueue = false;
  private boolean transactions = false;
  private long timeout = 5000;

  public Consumer() {
  }

  public void connectToQueue(final String url, final String queue) throws JMSException {
    ConnectionFactory connectionFactory =
            new ActiveMQConnectionFactory(url);
    connection = connectionFactory.createConnection();
    connection.start();
    session = connection.createSession(transactions,
            Session.CLIENT_ACKNOWLEDGE);
    destination = getQueue(session, queue);
    consumer = session.createConsumer(destination);
  }

  public void connectToQueue(final String queue, final Connection connection) throws JMSException {
    this.connection = connection;
    session = this.connection.createSession(transactions,
            Session.CLIENT_ACKNOWLEDGE);
    destination = getQueue(session, queue);
    consumer = session.createConsumer(destination);
  }

  /**
   *
   *
   * @author Michael Golowka
   * @param url
   * @param topic
   * @throws JMSException
   */
  public void connectToTopic(final String url, final String topic) throws JMSException {
    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
    connection = connectionFactory.createConnection();
    connection.start();
    session = connection.createSession(transactions, Session.CLIENT_ACKNOWLEDGE);
    destination = getTopic(session, topic);
    consumer = session.createConsumer(destination);
  }

  /**
   * Get destination object.
   *
   * @param session
   * @param queue
   * @return
   * @throws JMSException
   */
  public Destination getQueue(final Session session, final String queue) throws JMSException {
    Destination dest;
    if (tempQueue) {
      TemporaryQueue temporaryQueue = session.createTemporaryQueue();
      dest = temporaryQueue;
    } else {
      dest = session.createQueue(queue);
    }
    return dest;
  }

  /**
   * Get destination object.
   *
   * @param session
   * @param queue
   * @return
   * @throws JMSException
   */
  public Destination getTopic(final Session session, final String topic) throws JMSException {
    Destination dest;
    if (tempQueue) {
      TemporaryTopic temporaryTopic = session.createTemporaryTopic();
      dest = temporaryTopic;
    } else {
      dest = session.createTopic(topic);
    }
    return dest;
  }

  /**
   * Get a Text message.
   *
   * @return
   * @throws JMSException
   */
  public TextMessage getTextMessage() throws JMSException {
    Message msg = consumer.receive(timeout);
    if (msg != null) {
      if (!(msg instanceof TextMessage)) {
        throw new JMSException("Message object not of type TextMessage: " + msg.getClass());
      }

      return (TextMessage) msg;
    } else {
      return null;
    }
  }

  /**
   * Get a Map message.
   *
   * @return
   * @throws JMSException
   */
  public MapMessage getMapMessage() throws JMSException {
    return (MapMessage) consumer.receive(timeout);
  }

  /**
   * Returns the destination object, do not use this unless you know what you are doing.
   *
   * @return the destination
   */
  public Destination getDestination() {
    return destination;
  }

  public void setTimeout(final long t) {
    timeout = t;
  }

  public long getTimeout() {
    return timeout;
  }

  /**
   * Enable transactions on the queue. This must be called before connect to take affect.
   *
   * @param bool
   */
  public void setUseTransactions(final Boolean bool) {
    transactions = bool;
  }

  public void setUseTemporaryQueue(final boolean bool) {
    tempQueue = bool;
  }

  public void subscribeToTopic(final String url, final String topic) throws JMSException {
    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
    connection = connectionFactory.createConnection();
    connection.setClientID("mgolowka");
    connection.start();
    session = connection.createSession(transactions, Session.CLIENT_ACKNOWLEDGE);
//    destination = getTopic(session, topic);
    Topic t = session.createTopic(topic);

    consumer = session.createDurableSubscriber(t, "mgolowka.subscriber");
  }

  /**
   * Close our JMS connection.
   *
   * @throws JMSException
   */
  @Override
  public void close() throws JMSException {
    connection.close();
  }
}
