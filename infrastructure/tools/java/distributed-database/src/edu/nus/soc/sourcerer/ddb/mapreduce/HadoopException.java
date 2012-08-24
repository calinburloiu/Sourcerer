package edu.nus.soc.sourcerer.ddb.mapreduce;

public class HadoopException extends Exception {

  private static final long serialVersionUID = -7039950839775117672L;

  public HadoopException() {
    super();
  }

//  public HadoopException(String message, Throwable cause,
//      boolean enableSuppression, boolean writableStackTrace) {
//    super(message, cause, enableSuppression, writableStackTrace);
//  }

  public HadoopException(String message, Throwable cause) {
    super(message, cause);
  }

  public HadoopException(String message) {
    super(message);
  }

  public HadoopException(Throwable cause) {
    super(cause);
  }

}
