package com.vreco.util.profiling;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

public class Profiler {

  private static Logger                       logger          = Logger.getLogger(Profiler.class);
  private static Logger                       profilingLogger = Logger.getLogger("Profiling");
  private static final Map<String, StopWatch> profiles        = new HashMap<String, StopWatch>();
  private static Properties                   conf;
  private static boolean                      errored = false;

  public static void initialize(final Properties config) {
    conf = config;
  }

  public static synchronized void startProfile(String tagName) {
    if (conf == null) {
      if (!errored) {
        logger.error("Failed to initialize profiler");
        errored = true; // Prevent this message from happening a lot
      }
      return;
    }
    if (Boolean.parseBoolean(conf.getProperty("profiling.enabled", "false"))) {
      tagName = getTagName(tagName);
      if (profiles.get(tagName) != null) {
        profilingLogger.error("Unable to start profiling on a tag that has already started ["
            + tagName + "]");
        return;
      }

      StopWatch sw = new Log4JStopWatch(tagName, profilingLogger);
      profiles.put(tagName, sw);
    }
  }

  public static synchronized void stopProfile(String tagName) {
    if (conf == null) {
      if (!errored) {
        logger.error("Failed to initialize profiler");
        errored = true; // Prevent this message from happening a lot
      }
      return;
    }
    if (Boolean.parseBoolean(conf.getProperty("profiling.enabled", "false"))) {
      tagName = getTagName(tagName);
      if (profiles.get(tagName) == null) {
        profilingLogger.error("Unable to stop profile: no start detected on tag [" + tagName + "]");
        return;
      }

      profiles.get(tagName).stop();
      profiles.remove(tagName);
    }
  }

  private static String getTagName(final String tagName) {
    return Thread.currentThread().getId() + " - " + tagName;
  }
}
