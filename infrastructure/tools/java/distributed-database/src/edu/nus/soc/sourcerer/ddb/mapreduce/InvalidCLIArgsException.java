package edu.nus.soc.sourcerer.ddb.mapreduce;

public class InvalidCLIArgsException extends Exception {

  private static final long serialVersionUID = 6198753190963248829L;

  public InvalidCLIArgsException() {
    super();
  }

//  public InvalidCLIArgsException(String message, Throwable cause,
//      boolean enableSuppression, boolean writableStackTrace) {
//    super(message, cause, enableSuppression, writableStackTrace);
//  }

  public InvalidCLIArgsException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidCLIArgsException(String message) {
    super(message);
  }

  public InvalidCLIArgsException(Throwable cause) {
    super(cause);
  }

}
