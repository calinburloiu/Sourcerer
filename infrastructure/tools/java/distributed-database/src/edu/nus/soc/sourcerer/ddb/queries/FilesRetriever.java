package edu.nus.soc.sourcerer.ddb.queries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.FilesHBTable;
import edu.nus.soc.sourcerer.ddb.util.ModelAppender;
import edu.nus.soc.sourcerer.model.ddb.FileModel;

public class FilesRetriever {
  
  protected HTable table;
  
  public FilesRetriever() throws HBaseException {
    super();
    
    // Get a table instance.
    try {
      table = FilesHBTable.getInstance().getHTable();
    } catch (IOException e) {
      throw new HBaseConnectionException(
          "Could not connect to HBase for files table.", e);
    }
  }
  
  public FileModel getFileFromResult(Result result) {
    byte[] row = result.getRow();
    byte[] projectID = Bytes.head(row, 16);
    byte fileType = row[16];
    byte[] fileID = Bytes.tail(row, 16);
    
    byte[] name = result.getValue(FilesHBTable.CF_DEFAULT,
        FilesHBTable.COL_NAME);
    byte[] path = result.getValue(FilesHBTable.CF_DEFAULT,
        FilesHBTable.COL_PATH);
    byte[] hash = result.getValue(FilesHBTable.CF_DEFAULT,
        FilesHBTable.COL_HASH);
    byte[] jpid = result.getValue(FilesHBTable.CF_DEFAULT,
        FilesHBTable.COL_JARPID);
    byte[] loc = result.getValue(FilesHBTable.CF_METRICS,
        FilesHBTable.COL_METRIC_LOC);
    byte[] nwloc = result.getValue(FilesHBTable.CF_METRICS,
        FilesHBTable.COL_METRIC_NWLOC);
    
    return new FileModel(fileID, fileType, projectID,
        name == null ? null : Bytes.toString(name),
        path == null ? null : Bytes.toString(path), hash, jpid,
        loc == null ? null : Bytes.toInt(loc),
        nwloc == null ? null : Bytes.toInt(nwloc));
  }
  
  public void retrieveFiles(ModelAppender<FileModel> appender,
      byte[] projectID, Byte fileType, byte[] fileID)
      throws HBaseException {
    FileModel file = null;
    
    // Do a get HBase operation if all information is available.
    if (projectID != null && fileType != null && fileID != null) {
      Get get = new Get(Bytes.add(projectID, new byte[] {fileType}, fileID));
      get.addFamily(FilesHBTable.CF_DEFAULT);
      get.addFamily(FilesHBTable.CF_METRICS);
      Result result = null;
      
      try {
        result = table.get(get);
      } catch (IOException e) {
        throw new HBaseException(e.getMessage(), e);
      }
      
      file = getFileFromResult(result);
      appender.add(file);
    }
    else {
      // Compute the partial row key for scanning.
      ByteBuffer bb = ByteBuffer.allocate(33);
      if (projectID == null) {
        throw new IllegalArgumentException("project-id argument must be set.");
      }
      else {
        bb.put(projectID);
      }
      if (fileType != null) {
        bb.put(new byte[] {fileType});
      }
      if (fileID != null) {
        if (fileType == null)
          throw new IllegalArgumentException("file-type argument must be set if file-id is set.");
        bb.put(fileID);
      }
      int length = bb.position();
      byte[] startRow = new byte[length];
      bb.position(0);
      bb.get(startRow, 0, length);
      byte[] endRow = Arrays.copyOf(startRow, length);
      endRow[length-1]++;
      
      // Scan.
      Scan scan = new Scan(startRow, endRow);
      ResultScanner scanner = null;
      try {
        scanner = table.getScanner(scan);
        
        for (Result result : scanner) {
          appender.add(getFileFromResult(result));
        }
      } catch (IOException e) {
        throw new HBaseException(e.getMessage(), e);
      } finally {
        scanner.close();
      }
    }
    
  }

}
