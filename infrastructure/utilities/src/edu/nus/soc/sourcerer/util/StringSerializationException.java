package edu.nus.soc.sourcerer.util;

public class StringSerializationException extends RuntimeException {

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
