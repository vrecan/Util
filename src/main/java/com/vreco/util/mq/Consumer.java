package com.vreco.util.mq;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Basic connection library to connect to AMQ and consume messages.
 * @author Ben Aldrich
 */
public class Consumer {

  private MessageConsumer consumer;
  private Connection connection;
  private Session session;
  private Destination destination;
  private boolean tempQueue = false;
  private boolean transactions = false;
  private long timeout = 5000;

  public Consumer() {
  }

  public void connect(String url, String queue) throws JMSException {
    ConnectionFactory connectionFactory =
            new ActiveMQConnectionFactory(url);
    connection = connectionFactory.createConnection();
    connection.start();
    session = connection.createSession(transactions,
            Session.CLIENT_ACKNOWLEDGE);
    destination = getDestination(session, queue);
    consumer = session.createConsumer(destination);
  }

  public void connect(String queue, Connection connection) throws JMSException {
    this.connection = connection;
    session = this.connection.createSession(transactions,
            Session.CLIENT_ACKNOWLEDGE);
    destination = getDestination(session, queue);
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
  public Destination getDestination(final Session session, final String queue) throws JMSException {
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

  public void setTimeout(long t) {
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
  public void setUseTransactions(Boolean bool) {
    transactions = bool;
  }

  public void setUseTemporaryQueue(boolean bool) {
    tempQueue = bool;
  }

  /**
   * Close our JMS connection.
   *
   * @throws JMSException
   */
  public void close() throws JMSException {
    connection.close();
  }
}
