package edu.nus.soc.sourcerer.ddb.queries;

import static edu.uci.ics.sourcerer.util.io.Logging.logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.SourcesHBTable;
import edu.nus.soc.sourcerer.model.ddb.SourceModel;
import edu.nus.soc.sourcerer.util.StringSerializationException;

public class SourceModelInserter implements ModelInserter<SourceModel> {
  protected int selectRowsCount;
  protected HTable table;
  
  public SourceModelInserter(int selectRowsCount)
      throws HBaseConnectionException {
    super();
    this.selectRowsCount = selectRowsCount;

    // Get a table instance.
    try {
      table = SourcesHBTable.getInstance().getHTable();
    } catch (IOException e) {
      throw new HBaseConnectionException(
          "Could not connect to HBase for sources table.", e);
    }
    table.setAutoFlush(false);
  }

  @Override
  public void insertModels(Collection<SourceModel> models) throws HBaseException {
    ArrayList<Put> puts = new ArrayList<Put>(selectRowsCount);
    Put put = null;
    
    for (SourceModel file : models) {
      try {
//        logger.fine(file.getFileName());
        put = new Put(file.getFileName().getBytes("UTF-8"));
        
        put.add(SourcesHBTable.CF_DEFAULT, SourcesHBTable.COL_CONTENT,
            file.getContent().getBytes("UTF-8"));
        
        puts.add(put);
      } catch (UnsupportedEncodingException e) {
        throw new StringSerializationException(e.getMessage(), e);
      }
    }
    
    try {
      table.put(puts);
      table.flushCommits();
    } catch (IOException e) {
      throw new HBaseException(e);
    }
  }

}
