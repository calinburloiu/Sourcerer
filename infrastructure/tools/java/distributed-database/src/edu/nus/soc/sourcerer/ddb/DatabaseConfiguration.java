package edu.nus.soc.sourcerer.ddb;

import org.apache.hadoop.hbase.io.hfile.Compression;

/**
 * This class contains configuration information required during for operating
 * with HBase.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class DatabaseConfiguration {
  
  protected String tablePrefix = "";
  protected Compression.Algorithm defaultCompression = Compression.Algorithm.GZ;

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
