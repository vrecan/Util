package com.vreco.util.mq;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Basic connection library to connect to AMQ and consume messages.
 *
 * @author Ben Aldrich
 */
public class Consumer implements AutoCloseable {

  private MessageConsumer consumer;
  private Connection connection;
  private Session session;
  private Destination destination;
  private boolean tempQueue = false;
  private boolean transactions = false;
  private boolean persistence = false;
  private long timeout = 5000;
  private String url;

  public Consumer(String url) {
    this.url = url;
  }

  public void connect(String type, String queue) throws JMSException {
    setConnection();
    setSession();
    destination =  session.createQueue(queue);
    consumer = session.createConsumer(destination);
  }

  public void connect(String type, String queue, Connection connection) throws JMSException {
    this.connection = connection;
    setSession();
    destination =  session.createQueue(queue);
    consumer = session.createConsumer(destination);
  }

  protected void setSession() throws JMSException {
    if (session == null) {
      session = connection.createSession(transactions, Session.CLIENT_ACKNOWLEDGE);
    }
  }

  protected void setConnection() throws JMSException {
    ConnectionFactory connectionFactory =
            new ActiveMQConnectionFactory(url);
    connection = connectionFactory.createConnection();
    connection.start();
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
  @Override
  public void close() throws JMSException {
    try {
    if(consumer != null) {
      consumer.close();
    }
    if(session != null) {
      session.close();
    }
    if(connection != null) {
      connection.close();
    }
    } catch (Exception e) {
      //loghere
    }
  }
}
