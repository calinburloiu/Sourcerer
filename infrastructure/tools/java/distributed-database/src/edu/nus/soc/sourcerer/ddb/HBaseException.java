package edu.nus.soc.sourcerer.ddb;

public class HBaseException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = -3016323836987666181L;

  public HBaseException() {
  }

  public HBaseException(String message) {
    super(message);
  }

  public HBaseException(Throwable cause) {
    super(cause);
  }

  public HBaseException(String message, Throwable cause) {
    super(message, cause);
  }

}
