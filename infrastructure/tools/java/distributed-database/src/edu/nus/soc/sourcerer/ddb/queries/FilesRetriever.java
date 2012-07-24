package edu.nus.soc.sourcerer.ddb.queries;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.FilesHBTable;

public class FilesRetriever {
  
  protected HTable table;
  
  private FilesRetriever() throws HBaseException {
    super();
    
    // Get a table instance.
    try {
      table = FilesHBTable.getInstance().getHTable();
    } catch (IOException e) {
      throw new HBaseConnectionException(
          "Could not connect to HBase for files table.", e);
    }
  }
  
  

}
