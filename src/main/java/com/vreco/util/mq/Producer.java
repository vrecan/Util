package com.vreco.util.mq;

import com.vreco.util.Util;
import java.util.HashMap;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @author mgolowka
 */
public class Producer implements AutoCloseable {

  private String url;
  private ConnectionFactory connectionFactory;
  private Connection connection;
  private Session session;
  private Destination destination;
  private MessageProducer producer;
  private boolean persistentMsgs;
  private Destination replyTo;
  private HashMap<String, Destination> destinations = new HashMap();
  private HashMap<String, MessageProducer> producers = new HashMap();

  public Producer(final String url) {
    this.url = url;
  }

  /**
   * Connect to topc / queue.
   * @param type
   * @param queue
   * @throws JMSException 
   */
  public void connect(final String type, final String queue) throws JMSException {
    setConnection();
    setSession(type, queue);
    setDestination(type, queue);
    setProducer(type, queue);
  }

  /**
   * set our session.
   * @param type
   * @param queue
   * @throws JMSException 
   */
  protected void setSession(final String type, final String queue) throws JMSException {    
    if (session == null) {
      //true/false == persistant
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }    
  }


  /**
   * Set the destination, reuse old destinations if they exist.
   * @param type
   * @param destString
   * @throws JMSException 
   */
  protected void setDestination(final String type, final String destString) throws JMSException {
    if (destination == null) {
      setDestinationByType(type, destString);
    } else {
      Destination previousDestination = destinations.get(destString);
      if (previousDestination != null) {
        destination = previousDestination;
      } else {
        setDestinationByType(type, destString);
      }
    }
  }

  protected void setDestinationByType(final String type, final String destString) throws JMSException {
    switch (type) {
      case "queue":
        destination = session.createTopic(destString);
        destinations.put(destString, destination);
      case "topic":
        destination = session.createQueue(destString);
        destinations.put(destString, destination);
    }
  }

  /**
   * Set producer, re use old producers if it's for the same queue.
   * @param type
   * @param destString
   * @throws JMSException 
   */
  protected void setProducer(final String type, final String destString) throws JMSException {
    if (producer == null) {
      setProducerByType(type, destString);
    } else {
      MessageProducer previousProducer = producers.get(destString);
      if (previousProducer != null) {
        producer = previousProducer;
      } else {
        setProducerByType(type, destString);
      }
    }

  }

  protected void setProducerByType(final String type, final String destString) throws JMSException {
    switch (type) {
      case "queue":
        producer = session.createProducer(destination);
        producers.put(destString, producer);
      case "topic":
        producer = session.createProducer(destination);
        producers.put(destString, producer);
    }

  }

  @Override
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

  private void setConnection() throws JMSException {
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