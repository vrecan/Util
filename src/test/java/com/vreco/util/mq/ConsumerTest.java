package com.vreco.util.mq;

import javax.jms.Destination;
import junit.framework.TestCase;

/**
 *
 * @author Ben Aldrich
 */
public class ConsumerTest extends TestCase {
    
    public ConsumerTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

  /**
   * Test of connect method, of class Consumer.
   */
  public void testConnect() throws Exception {
    System.out.println("connect");
    String url = "vm://localhost";
    String queue = "tempQ";
    Consumer instance = new Consumer();
    instance.connect(url, queue);
    instance.close();
  }

  /**
   * Test of getDestination method, of class Consumer.
   */
  public void testGetDestination_Session_String() throws Exception {
    System.out.println("getDestination");
    String url = "vm://localhost";
    String queue = "tempQ";
    Consumer instance = new Consumer();
    instance.connect(url, queue);
    Destination destination = instance.getDestination();
    if(destination == null) {
      throw new Exception("Destination null");
    }
    instance.close();
  }
}
