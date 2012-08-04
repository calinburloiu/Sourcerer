package edu.nus.soc.sourcerer.util;

public class StringSerializationException extends RuntimeException {

  private static final long serialVersionUID = -4675975582399542920L;

  public StringSerializationException() {
  }

  public StringSerializationException(String message) {
    super(message);
  }

  public StringSerializationException(Throwable cause) {
    super(cause);
  }

  public StringSerializationException(String message, Throwable cause) {
    super(message, cause);
  }

}
