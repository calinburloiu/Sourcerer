package edu.nus.soc.sourcerer.ddb.tables;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;

public abstract class HBTable {
  
  protected HTable hTable = null;
  
  public abstract String getName();
  
  public void setupHTable() {
    
  }
  
  public HTable getHTable()
      throws IOException {
    if (getName() == null)
      throw new Error("HBase table class needs to declare a NAME.");
    
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    
    if (hTable == null) {
      Configuration conf = HBaseConfiguration.create();
      hTable = new HTable(conf, dbConf.getTablePrefix() + getName());
      setupHTable();
    }
    
    return hTable;
  }
  
  
}
