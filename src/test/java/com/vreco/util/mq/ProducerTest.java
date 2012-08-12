package com.vreco.util.mq;

import java.io.IOException;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.TextMessage;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.junit.Ignore;

/**
 *
 * @author Ben Aldrich
 */
public class ProducerTest extends TestCase {

  private final String vmUrl = "vm://localhost?broker.persistent=false";
  //private final String vmUrl = "tcp://localhost:61616?jms.prefetchPolicy.all=5";
  private Consumer consumer;
  private Producer producer;

  public ProducerTest(String testName) {
    super(testName);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    consumer = new Consumer(vmUrl);
    producer = new Producer(vmUrl);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    if (consumer != null) {
      consumer.close();
    }
    if (producer != null) {
      producer.close();
    }
  }

  /**
   * Test of connect method, of class Producer.
   */
  public void testConnect() throws Exception {

    producer.connect("queue", "testQ");
  }

  /**
   * Test of setDestination method, of class Producer.
   */
  public void testConnectMultiple() throws Exception {
    producer.connect("queue", "testQ");
    consumer.connect("queue", "testQ");
    producer.sendMessage("testT");
    consumer.setTimeout(2000);
    TextMessage msg = consumer.getTextMessage();
    msg.acknowledge();
    Assert.assertEquals("testT", msg.getText());
    consumer.connect("queue", "testQ2");
    producer.connect("queue", "testQ2");
    producer.sendMessage("testT2");
    consumer.setTimeout(2000);
    msg = consumer.getTextMessage();

    if (msg != null) {
      msg.acknowledge();
      Assert.assertEquals("testT2", msg.getText());
    } else {
      throw new IOException("NO message found!");
    }



  }

  /**
   * Test of setReplyTo method, of class Producer.
   */
  public void testSetReplyTo() throws Exception {

    producer.connect("queue", "testQ");
    consumer.connect("queue", "testQ");
    Destination exp = producer.getDestination();
    producer.sendMessage("testT", exp);
    consumer.setTimeout(2000);
    TextMessage msg = consumer.getTextMessage();
    if (msg != null) {
      msg.acknowledge();
      Assert.assertEquals("testT", msg.getText());
      Assert.assertEquals(exp, msg.getJMSReplyTo());
    } else {
      throw new IOException("No message found");
    }


  }

  /**
   * Test of setPersistentMsgs method, of class Producer.
   */
  public void testSetPersistentMsgsTrue() throws Exception {

    producer.connect("queue", "testQ");
    consumer.connect("queue", "testQ");
    producer.setPersistence(true);
    producer.sendMessage("testT");
    consumer.setTimeout(2000);
    TextMessage msg = consumer.getTextMessage();

    if (msg != null) {
      msg.acknowledge();
      Assert.assertEquals("testT", msg.getText());
      Assert.assertEquals(DeliveryMode.PERSISTENT, msg.getJMSDeliveryMode());
    } else {
      throw new IOException("NO message found!");
    }

  }

  /**
   * Test of setPersistentMsgs method, of class Producer.
   */
  @Ignore
  public void testSetPersistentMsgsFalse() throws Exception {

    producer.connect("queue", "testQ");
    consumer.connect("queue", "testQ");
    producer.setPersistence(false);
    producer.sendMessage("testT");
    consumer.setTimeout(2000);
    TextMessage msg = consumer.getTextMessage();
    if (msg != null) {
      msg.acknowledge();
      Assert.assertEquals("testT", msg.getText());
      //turning persistence off fails...
      Assert.assertEquals(DeliveryMode.NON_PERSISTENT, msg.getJMSDeliveryMode());
    } else {
      throw new IOException("NO message found!");
    }
  }

  /**
   * Test of testSetTTLMsg method, of class Producer.
   */
  public void testSetTTLMsgTrue() throws Exception {

    producer.connect("queue", "testQ");
    consumer.connect("queue", "testQ");
    producer.setTTL(100);
    producer.sendMessage("testT");
    Thread.sleep(300);
    consumer.setTimeout(2000);
    TextMessage msg = consumer.getTextMessage();

    if (msg != null) {
      throw new IOException("Message found when none should exist!");
    }
  }
  
  /**
   * Test of testSetTTLMsg method, of class Producer.
   */
  public void testSetTTLMsgFalse() throws Exception {

    producer.connect("queue", "testQ");
    consumer.connect("queue", "testQ");
    producer.setTTL(1000);
    producer.sendMessage("testT");
    Thread.sleep(300);
    consumer.setTimeout(2000);
    TextMessage msg = consumer.getTextMessage();
    if (msg == null) {
      throw new IOException("Message found when none should exist!");
    }
  }  
}