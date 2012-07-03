package edu.nus.soc.sourcerer.ddb;

public class HBaseConnectionException extends HBaseException {

  private static final long serialVersionUID = 2031773379280461628L;

  public HBaseConnectionException() {
  }

  public HBaseConnectionException(String message) {
    super(message);
  }

  public HBaseConnectionException(Throwable cause) {
    super(cause);
  }

  public HBaseConnectionException(String message, Throwable cause) {
    super(message, cause);
  }

}
