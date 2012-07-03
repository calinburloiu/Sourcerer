package edu.nus.soc.sourcerer.ddb;

import org.apache.hadoop.hbase.io.hfile.Compression;

/**
 * This singleton class contains configuration information required during for
 * operating with HBase.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class DatabaseConfiguration {
  
  private static DatabaseConfiguration instance = null;
  
  protected String tablePrefix = "";
  protected Compression.Algorithm defaultCompression = Compression.Algorithm.GZ;

  private DatabaseConfiguration() {
    super();
  }
  
  public static DatabaseConfiguration getInstance() {
    if (instance == null)
      instance = new DatabaseConfiguration();
    
    return instance;
  }

  public String getTablePrefix() {
    return tablePrefix;
  }

  public void setTablePrefix(String tablePrefix) {
    this.tablePrefix = tablePrefix;
  }

  public Compression.Algorithm getDefaultCompression() {
    return defaultCompression;
  }

  public void setDefaultCompression(Compression.Algorithm defaultCompression) {
    this.defaultCompression = defaultCompression;
  }
  
}
