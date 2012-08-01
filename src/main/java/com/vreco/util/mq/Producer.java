package com.vreco.util.mq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.vreco.util.Util;

/**
 * @author mgolowka
 */
public class Producer implements AutoCloseable {

  private String            url;
  private ConnectionFactory connectionFactory;
  private Connection        connection;
  private Session           session;
  private Destination       destination;
  private MessageProducer   producer;
  private boolean           persistentMsgs;
  private Destination       replyTo;

  public Producer(final String url) {
    this.url = url;
  }

  /**
   * Connect to a Queue
   *
   * @param queue
   * @param autoAcknowledge
   * @param persistent
   * @param replyTo
   * @throws JMSException
   */
  public void connectToQueue(final String queue, final boolean autoAcknowledge,
      final boolean persistent, final Destination replyTo) throws JMSException {
    createConnFactory();

    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    destination = session.createQueue(queue);

    producer = session.createProducer(destination);

    this.persistentMsgs = persistent;
    this.replyTo = replyTo;
  }

  /**
   * Connect to topic
   *
   * @param topic
   * @param autoAcknowledge
   * @param persistent
   * @param replyTo
   * @throws JMSException
   */
  public void connectToTopic(final String topic, final boolean autoAcknowledge,
      final boolean persistent, final Destination replyTo) throws JMSException {
    createConnFactory();

    session = connection.createSession(false, (autoAcknowledge ? Session.AUTO_ACKNOWLEDGE
        : Session.CLIENT_ACKNOWLEDGE));
    destination = session.createTopic(topic);

    producer = session.createProducer(destination);

    this.persistentMsgs = persistent;
    this.replyTo = replyTo;
  }

  public void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (JMSException e) {
        System.out.println(Util.getStackTrace("Unable to close connection", e));
      }
      connection = null;
    }
  }

  public void sendMessage(final String message) throws JMSException {
    if (message == null) {
      return;
    }
    Message msg = session.createTextMessage(message);
    if (persistentMsgs) {
      msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
    } else {
      msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
    }

    msg.setJMSReplyTo(replyTo);
    producer.send(msg);
  }

  private void createConnFactory() throws JMSException {
    if (connectionFactory == null) {
      connectionFactory = new ActiveMQConnectionFactory(url);
    }

    if (connection == null) {
      connection = connectionFactory.createConnection();
      connection.start();
    }
  }

  public void setReplyTo(final Destination replyTo) {
    this.replyTo = replyTo;
  }

  public void setPersistentMsgs(final boolean persistentMsgs) {
    this.persistentMsgs = persistentMsgs;
  }
}