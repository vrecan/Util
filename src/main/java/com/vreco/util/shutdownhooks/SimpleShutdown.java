
package com.vreco.util.shutdownhooks;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 * Used as a shutdown hook to the main daemon thread to prevent the daemon from
 * shutting down before all other threads have finished shutting down properly.
 *
 * @author Baldrich
 */
public class SimpleShutdown extends Thread {

  private static SimpleShutdown instance = null;
  private AtomicBoolean finished = new AtomicBoolean(false);
  private AtomicBoolean shutdown = new AtomicBoolean(false);
  private static Logger logger = Logger.getLogger(SimpleShutdown.class);

  protected SimpleShutdown() {
    // Exists only to defeat instantiation.
    setName("Shutdown thread");
  }

  /**
   * Should we be shutting down?
   * @return 
   */
  public Boolean isShutdown() {
    return this.shutdown.get();
  }

  /**
   * Set atomic boolean for shutdown.
   * @param shutdown 
   */
  public void setShutdown(final boolean shutdown) {
    this.shutdown.set(shutdown);
  }

  /**
   * Are we finished shutting down.
   * @return 
   */
  public boolean isFinished() {
    return this.finished.get();
  }

  /**
   * Set the finished atomic boolean.
   * @param finished 
   */
  public void setFinished(final boolean finished) {
    this.finished.set(finished);
  }

  
  public static SimpleShutdown getInstance() {
    if(instance == null) {
      synchronized(SimpleShutdown.class) {
        instance = new SimpleShutdown();
      }
    }
    return instance;
  }

  /**
   * This is the method called when a signal is received.
   */
  @Override
  public void run() {
    this.setShutdown(true);
    logger.info("Caught signal to shutdown");
    while (!this.isFinished()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
      }
    }
    logger.info("Finished shutdown");

    LogManager.shutdown();
  }
}