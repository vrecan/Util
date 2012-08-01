package com.vreco.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author mgolowka
 */
public class Util {
  public static final String lineSeparator = System.getProperty("line.separator");

  public static String getStackTrace(final String msg, final Throwable ex) {
    return (msg == null || msg.isEmpty() ? "" : msg + lineSeparator) + getStackTrace(ex);
  }

  public static String getStackTrace(final Throwable ex) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
    return sw.toString(); // stack trace as a string
  }

  public static String getStackTrace() {
    // Remove the first element because it will always be a reference to the Util.getStackTrace()
    // method, which we don't care about. Technically this can be twice because of the return call
    // here, but I'm not worrying about that for now
    StackTraceElement[] stes = Thread.currentThread().getStackTrace();
    StackTraceElement[] trimmedStes = new StackTraceElement[stes.length - 1];
    for (int i = 1; i < trimmedStes.length; i++) {
      trimmedStes[i - 1] = stes[i];
    }
    return getStackTrace(trimmedStes);
  }

  public static String getStackTrace(final StackTraceElement stackTrace[]) {
    StringBuilder str = new StringBuilder();
    for (StackTraceElement ste : stackTrace) {
      str.append(ste);
      str.append(lineSeparator);
    }
    return str.toString();
  }

  /**
   * Joins the objects together into a single string. This was built to be very similar to the Perl
   * join() function
   *
   * @param delimiter
   * @param objs
   * @return
   * @see Util#join(String, Collection)
   */
  public static String join(final String delimiter, final Object... objs) {
    ArrayList<Object> list = new ArrayList<Object>();
    for (Object obj : objs) {
      list.add(obj);
    }
    return join(delimiter, list);
  }

  /**
   * Convenience function that calls the join(String, Object...) method. This is primarily to get
   * rid of the annoying compiler warning in the EmailEngine
   *
   * @param delimiter
   * @param objs
   * @return
   * @see Util#join(String, Object...)
   */
  public static String join(final String delimiter, final String... objs) {
    return join(delimiter, (Object[]) objs);
  }

  /**
   * Joins the collection together into a single string. This was built to be very similar to the
   * Perl join() function
   *
   * @param delimiter
   * @param objs
   * @return
   */
  public static String join(final String delimiter, final Collection<?> collection) {
    if (collection == null) {
      return "";
    }

    // Remove nulls and empty strings from the list
    ArrayList<String> cleanedList = new ArrayList<String>();
    for (Object obj : collection) {
      if (obj != null) {
        String val = String.valueOf(obj);
        if (val == null || val.trim().isEmpty()) {
          continue;
        }
        cleanedList.add(val);
      }
    }

    StringBuilder str = new StringBuilder();

    boolean addDelim = false;
    for (String val : cleanedList) {
      if (addDelim && delimiter != null) {
        str.append(delimiter);
      }
      str.append(val);
      addDelim = true;
    }

    return str.toString();
  }

  /**
   * Joins the collections together into a single string. This was built to be very similar to the
   * Perl join() function
   *
   * @param delimiter
   * @param objs
   * @return
   * @see Util#join(String, Collection)
   */
  public static String join(final String delimiter, final Collection<?>... objs) {
    // objs is never null, but it can be 0 length
    if (objs.length == 0) {
      return "";
    }

    ArrayList<String> cleanedList = new ArrayList<String>();

    for (Collection<?> col : objs) {
      String collectionStr = join(delimiter, col);
      cleanedList.add(collectionStr);
    }

    return join(delimiter, cleanedList);
  }
}
