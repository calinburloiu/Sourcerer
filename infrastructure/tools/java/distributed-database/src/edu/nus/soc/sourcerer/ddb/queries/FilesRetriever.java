package edu.nus.soc.sourcerer.ddb.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.FilesHBTable;
import edu.nus.soc.sourcerer.ddb.util.BinaryRangeComparator;
import edu.nus.soc.sourcerer.ddb.util.ModelAppender;
import edu.nus.soc.sourcerer.model.ddb.FileModel;
import edu.nus.soc.sourcerer.util.EnumUtil;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.File;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

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
  
  public static FileModel resultToFileModel(Result result) {
    if (result == null || result.isEmpty()) {
      return null;
    }
    
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
  
  public void retrieveFilesWithGet(ModelAppender<FileModel> appender,
      byte[] projectID, Byte fileType, byte[] fileID)
  throws HBaseException {
    File[] types;
    // 000
    if (fileType != null) {
      LOG.info("Using Get operation");
      types = new File[] {
          (File) EnumUtil.getEnumByValue(File.values(), fileType) };
    }
    // 010
    else {
      LOG.info("Using Get operation multiple times for each ft");
      types = File.values();
    }
    
    List<Row> batch = new ArrayList<Row>(types.length);
    Object[] results = new Object[types.length];
    
    Get get = null;
    for (File crtType : types) {
      get = new Get(Bytes.add(projectID, new byte[] {crtType.getValue()},
          fileID));
      get.addFamily(FilesHBTable.CF_DEFAULT);
      get.addFamily(FilesHBTable.CF_METRICS);
      batch.add(get);
    }
    
    try {
      table.batch(batch, results);
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    } catch (InterruptedException e) {}
    
    // Only one file can be found (ID is unique).
    FileModel file = null;
    for (Object result : results) {
      if (result != null && !((Result) result).isEmpty()) {
        file = resultToFileModel((Result) result);
        appender.add(file);
        break;
      }
    }
  }
  
  public void retrieveFiles(ModelAppender<FileModel> appender,
      byte[] projectID, Byte fileType, byte[] fileID)
  throws HBaseException {
    // 0x0
    if (projectID != null && fileID != null) {
      retrieveFilesWithGet(appender, projectID, fileType, fileID);
      return;
    }
    
    Scan scan = null;
    byte[] startRow = null;
    ResultScanner scanner = null;
    
    // 001
    if (projectID != null && fileType != null && fileID == null) {
      LOG.info("Scanning by pid ft");
      startRow = Bytes.add(projectID, new byte[] {fileType});
      scan = new Scan(startRow, Serialization.incBytes(startRow));
    }
    // 011
    else if (projectID != null && fileType == null && fileID == null) {
      LOG.info("Scanning by pid");
      scan = new Scan(projectID, Serialization.incBytes(projectID));
    }
    else {
      scan = new Scan();
      
      // 100
      if (projectID == null && fileType != null && fileID != null) {
        LOG.info("Scanning the whole table and filtering by ft fid");
        BinaryRangeComparator brc = new BinaryRangeComparator(
            Bytes.add(new byte[] {fileType}, fileID), 16, false);
        Filter filter = new RowFilter(CompareOp.EQUAL, brc);
        scan.setFilter(filter);
      }
      // 101
      else if (projectID == null && fileType != null && fileID == null) {
        LOG.info("Scanning the whole table and filtering by ft");
        BinaryRangeComparator brc = new BinaryRangeComparator(
            new byte[] {fileType}, 16, false);
        Filter filter = new RowFilter(CompareOp.EQUAL, brc);
        scan.setFilter(filter);
      }
      // 110
      else if (projectID == null && fileType == null && fileID != null) {
        LOG.info("Scanning the whole table and filtering by fid");
        BinaryRangeComparator brc = new BinaryRangeComparator(
            fileID, 17, false);
        Filter filter = new RowFilter(CompareOp.EQUAL, brc);
        scan.setFilter(filter);
      }
      // 111
      else if (projectID == null && fileType == null && fileID == null) {
        LOG.info("Scanning the whole table");
      }
    }
    
    try {
      scanner = table.getScanner(scan);
      
      for (Result result : scanner) {
        if (!appender.add(resultToFileModel(result))) {
          break;
        }
      }
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    } finally {
      if (scanner != null)
        scanner.close();
    }

  }
  
}
