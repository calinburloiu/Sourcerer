package edu.nus.soc.sourcerer.ddb.queries;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.FilesHBTable;
import edu.nus.soc.sourcerer.model.ddb.FileModel;
import edu.nus.soc.sourcerer.util.StringSerializationException;

public class FileModelInserter implements ModelInserter<FileModel> {
  protected int selectRowsCount;
  protected HTable table;
  
  public FileModelInserter(int selectRowsCount)
      throws HBaseConnectionException {
    super();
    this.selectRowsCount = selectRowsCount;

    // Get a table instance.
    try {
      table = FilesHBTable.getInstance().getHTable();
    } catch (IOException e) {
      throw new HBaseConnectionException(
          "Could not connect to HBase for files table.", e);
    }
    table.setAutoFlush(false);
  }
  
  @Override
  public void insertModels(Collection<FileModel> models) throws HBaseException {
    ArrayList<Put> puts = new ArrayList<Put>(selectRowsCount);
    Put put = null;
    
    // Add each project to HBase.
    for (FileModel file : models) {
      try {
        put = new Put(Bytes.add(file.getProjectID(),
            new byte[] {file.getType().getValue()}, file.getId()));
        
        // Default column family
        put.add(FilesHBTable.CF_DEFAULT, FilesHBTable.COL_NAME,
            file.getName().getBytes("UTF-8"));
        if (file.getPath() != null) {
          put.add(FilesHBTable.CF_DEFAULT, FilesHBTable.COL_PATH,
              file.getPath().getBytes("UTF-8"));
        }
        if (file.getHash() != null) {
          put.add(FilesHBTable.CF_DEFAULT, FilesHBTable.COL_HASH,
              file.getHash());
        }
        if (file.getJarProjectID() != null) {
          put.add(FilesHBTable.CF_DEFAULT, FilesHBTable.COL_JARPID,
              file.getJarProjectID());
        }
        
        // Metrics column family
        if (file.getLoc() != null) {
          put.add(FilesHBTable.CF_METRICS, FilesHBTable.COL_METRIC_LOC,
              Bytes.toBytes(file.getLoc()));
        }
        if (file.getNwloc() != null) {
          put.add(FilesHBTable.CF_METRICS, FilesHBTable.COL_METRIC_NWLOC,
              Bytes.toBytes(file.getNwloc()));
        }
        
        puts.add(put);
      } catch (UnsupportedEncodingException e) {
        throw new StringSerializationException(e);
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
