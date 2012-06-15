package edu.nus.soc.sourcerer.ddb;

import java.io.Closeable;
import java.io.IOException;

public class DatabaseConnection implements Closeable {

  private static DatabaseConnection instance = null;
  
  protected DatabaseConnection() { }
  
  public static DatabaseConnection getInstance() {
    if (instance == null)
      instance = new DatabaseConnection();
    
    return instance;
  }
  
  public void open() throws IOException {
    
  }
  
  @Override
  public void close() throws IOException {

  }

}
