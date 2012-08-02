package com.vreco.util.mq;

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
  private boolean persistence = false;
  private boolean transactions = false;
  private HashMap<String, Destination> destinations = new HashMap();
  private HashMap<String, MessageProducer> producers = new HashMap();

  public Producer(final String url) {
    this.url = url;
  }

  /**
   * Connect to topic / queue.
   *
   * @param type
   * @param queue
   * @throws JMSException
   */
  public void connect(final String type, final String queue) throws JMSException {
    setConnection();
    setSession();
    setDestination(type, queue);
    setProducer(type, queue);
  }

  /**
   * set our session.
   *
   * @param type
   * @param queue
   * @throws JMSException
   */
  protected void setSession() throws JMSException {
    if (session == null) {
      session = connection.createSession(transactions, Session.AUTO_ACKNOWLEDGE);
    }
  }

  /**
   * Set the destination, reuse old destinations if they exist.
   *
   * @param type
   * @param destString
   * @throws JMSException
   */
  protected void setDestination(final String type, final String destString) throws JMSException {
    Destination previousDestination = destinations.get(destString);
    if (previousDestination != null) {
      destination = previousDestination;
    } else {
      setDestinationByType(type, destString);
    }
  }

  protected void setDestinationByType(final String type, final String destString) throws JMSException {
    switch (type) {
      case "queue":
        destination = session.createQueue(destString);
        destinations.put(destString, destination);
        break;
      case "topic":
        destination = session.createTopic(destString);
        destinations.put(destString, destination);
        break;
    }
  }

  /**
   * Set producer, re use old producers if it's for the same queue.
   *
   * @param type
   * @param destString
   * @throws JMSException
   */
  protected void setProducer(final String type, final String destString) throws JMSException {
    MessageProducer previousProducer = producers.get(destString);
    if (previousProducer != null) {
      producer = previousProducer;
    } else {
      setProducerWithDestination(destString);
    }
    setPersistence(persistence);
  }

  protected void setProducerWithDestination(final String destString) throws JMSException {
    producer = session.createProducer(destination);
    producers.put(destString, producer);

  }

  public void sendMessage(final String message) throws JMSException {
    if (message == null) {
      return;
    }
    Message msg = session.createTextMessage(message);
    producer.send(msg);
  }

  public void sendMessage(final String message, final Destination reply) throws JMSException {
    if (message == null) {
      return;
    }
    Message msg = session.createTextMessage(message);
    if (reply != null) {
      msg.setJMSReplyTo(reply);
    }
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

  public void setPersistence(final boolean persistence) throws JMSException {
    this.persistence = persistence;
    if (producer != null) {
      if (persistence) {
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
      } else {
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      }
    }
  }

  public Destination getDestination() {
    return destination;
  }
  
  @Override
  public void close() {
    try {
      if (producer != null) {
        producer.close();
      }
      if (session != null) {
        session.close();
      }
      if (connection != null) {
        connection.close();
      }
    } catch (Exception e) {
      //loghere
    }
  }  
}
