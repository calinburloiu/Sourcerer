package edu.nus.soc.sourcerer.ddb.util;

import java.io.PrintStream;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

public class PrintModelAppender<T> implements ModelAppender<T> {

  int count = 0;
  int limit = Integer.MAX_VALUE;
  boolean printCount;
  PrintStream printStream;
  
  public PrintModelAppender() {
    this(true, null, Integer.MAX_VALUE);
  }

  /**
   * If you pass null to printStream the logger will be used.
   * 
   * @param printCount
   * @param printStream
   */
  public PrintModelAppender(boolean printCount, PrintStream printStream,
      int limit) {
    super();
    this.printCount = printCount;
    this.printStream = printStream;
    this.limit = limit;
  }

  @Override
  public boolean add(T model) {
    if (model == null)
      return true;
    if (count >= limit)
      return false;

    count++;
    
    String strCount = "";
    if (printCount)
      strCount = count + ") ";
    
    if (printStream != null)
      printStream.println(strCount + model.toString());
    else
      LOG.info(strCount + model.toString());
    return true;
  }

  public boolean isPrintCount() {
    return printCount;
  }

  public void setPrintCount(boolean printCount) {
    this.printCount = printCount;
  }

  public PrintStream getPrintStream() {
    return printStream;
  }

  public void setPrintStream(PrintStream printStream) {
    this.printStream = printStream;
  }

  public int getCount() {
    return count;
  }

}
