package edu.nus.soc.sourcerer.ddb.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.FilesHBTable;
import edu.nus.soc.sourcerer.ddb.tables.RelationsDirectHBTable;
import edu.nus.soc.sourcerer.ddb.tables.RelationsHashHBTable;
import edu.nus.soc.sourcerer.ddb.tables.RelationsInverseHBTable;
import edu.nus.soc.sourcerer.ddb.util.BinaryRangeComparator;
import edu.nus.soc.sourcerer.ddb.util.ModelAppender;
import edu.nus.soc.sourcerer.model.ddb.RelationExtraModel;
import edu.nus.soc.sourcerer.model.ddb.Model;
import edu.nus.soc.sourcerer.model.ddb.RelationModel;
import edu.nus.soc.sourcerer.model.ddb.RelationsGroupedModel;
import edu.nus.soc.sourcerer.model.ddb.SourcedRelationsModel;
import edu.nus.soc.sourcerer.util.EnumUtil;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.File;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

public class RelationsRetriever {
  
  protected HTable relationsHashTable;
  protected HTable relationsDirectTable;
  protected HTable relationsInverseTable;
  protected HTable filesTable;

  public RelationsRetriever() throws HBaseException {
    super();

    try {
      relationsHashTable = RelationsHashHBTable.getInstance().getHTable();
      relationsDirectTable = RelationsDirectHBTable.getInstance().getHTable();
      relationsInverseTable = RelationsInverseHBTable.getInstance().getHTable();
      filesTable = FilesHBTable.getInstance().getHTable();
    } catch (IOException e) {
      throw new HBaseConnectionException(
          "Could not connect to HBase for relations tables.", e);
    }
  }
  
  public static RelationModel relationsHashResultToRelationModel(Result result) {
    if (result == null)
      return null;
    
    byte[] relationID = result.getRow();
    if (relationID == null)
      return null;
    
    byte[] kind = result.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_KIND);
    byte[] sourceID = result.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_SOURCE_ID);
    byte[] sourceType = result.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_SOURCE_TYPE);
    byte[] targetID = result.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_TARGET_ID);
    byte[] targetType = result.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_TARGET_TYPE);
    byte[] projectID = result.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_PROJECT_ID);
    byte[] fileID = result.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_FILE_ID);
    byte[] fileType = result.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_FILE_TYPE);
    byte[] offset = result.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_OFFSET);
    byte[] length = result.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_LENGTH);
    
    return new RelationModel(
        kind != null ? kind[0]
            : (byte) (Relation.UNKNOWN.getValue() | RelationClass.UNKNOWN.getValue()),
        sourceID, targetID, projectID, fileID,
        offset != null ? Bytes.toInt(offset) : null,
        length != null ? Bytes.toInt(length) : null,
        sourceType != null ? sourceType[0] : Entity.UNKNOWN.getValue(),
        targetType != null ? targetType[0] : Entity.UNKNOWN.getValue(),
        fileType != null ? fileType[0] : File.UNKNOWN.getValue());
  }
  
  public static List<RelationsGroupedModel>
      relationsDirectResultToRelationsGroupedModels(Result result) {
    if (result == null)
      return null;
    
    byte[] row = result.getRow();
    if (row == null)
      return null;
    byte[] sourceID = Bytes.head(row, 16);
    byte kind = row[16];
    byte[] targetID = Bytes.tail(row, 16);
    
    return getRelationsCols(sourceID, kind, targetID, result);
  }
  
  public static List<RelationsGroupedModel>
      relationsInverseResultToRelationsGroupedModels(Result result) {
    if (result == null)
      return null;
    
    byte[] row = result.getRow();
    if (row == null)
      return null;
    byte[] targetID = Bytes.head(row, 16);
    byte kind = row[16];
    byte[] sourceID = Bytes.tail(row, 16);
    
    return getRelationsCols(sourceID, kind, targetID, result);
  }
  
  public static List<RelationsGroupedModel> getRelationsCols(byte[] sourceID,
      byte kind, byte[] targetID, Result result) {
    NavigableMap<byte[], byte[]> map = result.getFamilyMap(
        RelationsDirectHBTable.CF_DEFAULT);
    Set<Entry<byte[], byte[]>> entrySet = map.entrySet();
    List<RelationsGroupedModel> groups =
        new ArrayList<RelationsGroupedModel>(map.size());
    RelationsGroupedModel group = null;
    byte[] column = null, value = null;
    byte[] projectID = null, fileID = null;
    List<RelationExtraModel> extra = null;
    
    for (Entry<byte[], byte[]> entry : entrySet) {
      column = entry.getKey();
      projectID = Bytes.head(column, 16);
      fileID = Bytes.tail(column, 16);
      
      value = entry.getValue();
      extra = RelationExtraModel.getExtraFromBytes(value);
      group = new RelationsGroupedModel(kind, sourceID, targetID, projectID,
          fileID, null, extra);
      groups.add(group);
    }
    
    return groups;
  }
  
  public static List<RelationsGroupedModel> filesResultToRelationsGroupedModels(
      Result result) {
    if (result == null)
      return null;
    
    byte[] row = result.getRow();
    if (row == null)
      return null;
    byte[] projectID = Bytes.head(row, 16);
    byte fileType = row[16];
    byte[] fileID = Bytes.tail(row, 16);
    
    return getFilesRelationsCols(projectID, fileType, fileID, result);
  }
  
  public static List<RelationsGroupedModel> getFilesRelationsCols(
      byte[] projectID, byte fileType, byte[] fileID, Result result) {
    NavigableMap<byte[], byte[]> map = result.getFamilyMap(
        FilesHBTable.CF_RELATIONS);
    Set<Entry<byte[], byte[]>> entrySet = map.entrySet();
    List<RelationsGroupedModel> groups =
        new ArrayList<RelationsGroupedModel>(map.size());
    RelationsGroupedModel group = null;
    byte[] column = null, value = null;
    byte kind;
    byte[] targetID = new byte[16], sourceID = null;
    
    for (Entry<byte[], byte[]> entry : entrySet) {
      column = entry.getKey();
      kind = column[0];
      System.arraycopy(column, 1, targetID, 0, 16);
      sourceID = Bytes.tail(column, 16);
      
      value = entry.getValue();
      List<RelationExtraModel> extra =
          RelationExtraModel.getExtraFromBytes(value);
      group = new RelationsGroupedModel(kind, sourceID, targetID, projectID,
          fileID, fileType, extra);
      groups.add(group);
    }
    
    return groups;
  }
  
  public static SourcedRelationsModel entitiesHashResultToSourcedRelationsModel(
      Result result) {
    return EntitiesRetriever.entitiesHashResultToSourcedRelationsModel(result);
  }
  
  public void retrieveSourcedRelations(
      ModelAppender<Model> appender, byte[] sourceID)
          throws HBaseException {
    (new EntitiesRetriever()).retrieveEntity(appender, sourceID, true);
  }
  
  public void retrieveRelationsWithGet(ModelAppender<Model> appender,
      byte[] sourceID, Byte kind, byte[] targetID,
      byte[] projectID, byte[] fileID)
  throws HBaseException {
    if (sourceID == null && kind == null && targetID == null) {
      return;
    }
    
    Random rnd = new Random(System.currentTimeMillis());
    boolean direct = rnd.nextBoolean();
    
    HTable table = null;
    Get get = null;
    byte[] family = null;
    if (direct) {
      LOG.info("relations_direct table: Using Get operation");
      table = relationsDirectTable;
      get = new Get(Bytes.add(sourceID, new byte[] {kind}, targetID));
      family = RelationsDirectHBTable.CF_DEFAULT;
    }
    else {
      LOG.info("relations_inverse table: Using Get operation");
      table = relationsInverseTable;
      get = new Get(Bytes.add(targetID, new byte[] {kind}, sourceID));
      family = RelationsInverseHBTable.CF_DEFAULT;
    }
    
    if (projectID != null && fileID != null) {
      get.addColumn(family, Bytes.add(projectID, fileID));
    }
    else {
      get.addFamily(family);
      
      if (projectID != null && fileID == null) {
        LOG.info("Filtering columns by pid.");
        Filter filter = new QualifierFilter(CompareOp.EQUAL,
            new BinaryPrefixComparator(projectID));
        get.setFilter(filter);
      }
      // FIXME not working
      else if (projectID == null && fileID != null) {
        LOG.info("Filtering columns by fid.");
        Filter filter = new QualifierFilter(CompareOp.EQUAL,
            new BinaryRangeComparator(fileID, 16, false));
        get.setFilter(filter);
      }
      else if (projectID == null && fileID == null) {
        LOG.info("No column filtering."); 
      }
    }
    
    Result result = null;
    try {
      result = table.get(get);
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    }
    
    if (result.isEmpty())
      LOG.info("No results found.");
    
    List<RelationsGroupedModel> groups = null;
    if (direct) {
      groups = relationsDirectResultToRelationsGroupedModels(result);
    }
    else {
      groups = relationsInverseResultToRelationsGroupedModels(result);
    }
    if (groups == null) {
      return;
    }
    for (RelationsGroupedModel group : groups) {
      if (!appender.add(group)) {
        break;
      }
    }
  }
  
  public void retrieveRelationsFromRelationsDirectTable(
      ModelAppender<Model> appender, byte[] sourceID,
      Byte kind, byte[] targetID, byte[] projectID, byte[] fileID)
  throws HBaseException {
    if (sourceID == null) {
      return;
    }
    
    Scan scan = null;
    byte[] startRow = null;
    List<Filter> filters = new ArrayList<Filter>(2);
    ResultScanner scanner = null;
    
    // 01
    if (kind != null && targetID == null) {
      LOG.info("relations_direct table: Scanning by seid rk");
      startRow = Bytes.add(sourceID, new byte[] {kind});
      scan = new Scan(startRow, Serialization.incBytes(startRow));
    }
    // 10
    else if (kind == null && targetID != null) {
      LOG.info("relations_direct table: Scanning by seid and filtering by teid");
      startRow = sourceID;
      scan = new Scan(startRow, Serialization.incBytes(startRow));
      BinaryRangeComparator brc = new BinaryRangeComparator(
          targetID, 16, true);
      Filter filter = new RowFilter(CompareOp.EQUAL, brc);
      filters.add(filter);
    }
    // 11
    else if (kind == null && targetID == null) {
      LOG.info("relations_direct table: Scanning by seid");
      startRow = sourceID;
      scan = new Scan(startRow, Serialization.incBytes(startRow));
    }
    
    // Filter by projectID or fileID if required.
    if (projectID != null && fileID != null) {
      scan.addColumn(RelationsDirectHBTable.CF_DEFAULT,
          Bytes.add(projectID, fileID));
    }
    else {
      scan.addFamily(RelationsDirectHBTable.CF_DEFAULT);
      
      if (projectID != null && fileID == null) {
        LOG.info("Filtering columns by pid.");
        Filter filter = new QualifierFilter(CompareOp.EQUAL,
            new BinaryPrefixComparator(projectID));
        filters.add(filter);
      }
      // FIXME not working
      else if (projectID == null && fileID != null) {
        LOG.info("Filtering columns by fid.");
        Filter filter = new QualifierFilter(CompareOp.EQUAL,
            new BinaryRangeComparator(fileID, 16, false));
        filters.add(filter);
      }
      else if (projectID == null && fileID == null) {
        LOG.info("No column filtering."); 
      }
    }
    
    // Set a filter if required.
    if (!filters.isEmpty()) {
      Filter filterList = new FilterList(filters);
      scan.setFilter(filterList);
    }
    
    try {
      scanner = relationsDirectTable.getScanner(scan);
      
      boolean stop = false;
      List<RelationsGroupedModel> groups = 
          new ArrayList<RelationsGroupedModel>();
      
      for (Result result : scanner) {
        if (stop == true) {
          break;
        }
        
        groups = relationsDirectResultToRelationsGroupedModels(result);
        for (RelationsGroupedModel group : groups) {
          if (!appender.add(group)) {
            stop = true;
            break;
          }
        }
      }
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    } finally {
      if (scanner != null)
        scanner.close();
    }
    
  }
  
  public void retrieveRelationsFromRelationsInverseTable(
      ModelAppender<Model> appender, byte[] sourceID,
      Byte kind, byte[] targetID, byte[] projectID, byte[] fileID)
  throws HBaseException {
    if (targetID == null) {
      return;
    }
    
    Scan scan = null;
    byte[] startRow = null;
    List<Filter> filters = new ArrayList<Filter>(2);
    ResultScanner scanner = null;
    
    // 01
    if (kind != null && sourceID == null) {
      LOG.info("relations_inverse table: Scanning by teid rk");
      startRow = Bytes.add(targetID, new byte[] {kind});
      scan = new Scan(startRow, Serialization.incBytes(startRow));
    }
    // 10
    else if (kind == null && sourceID != null) {
      LOG.info("relations_inverse table: Scanning by teid and filtering by teid");
      startRow = targetID;
      scan = new Scan(startRow, Serialization.incBytes(startRow));
      BinaryRangeComparator brc = new BinaryRangeComparator(
          sourceID, 16, true);
      Filter filter = new RowFilter(CompareOp.EQUAL, brc);
      filters.add(filter);
    }
    // 11
    else if (kind == null && sourceID == null) {
      LOG.info("relations_inverse table: Scanning by teid");
      startRow = targetID;
      scan = new Scan(startRow, Serialization.incBytes(startRow));
    }
    
    // Filter by projectID or fileID if required.
    if (projectID != null && fileID != null) {
      scan.addColumn(RelationsInverseHBTable.CF_DEFAULT,
          Bytes.add(projectID, fileID));
    }
    else {
      scan.addFamily(RelationsInverseHBTable.CF_DEFAULT);
      
      if (projectID != null && fileID == null) {
        LOG.info("Filtering columns by pid.");
        Filter filter = new QualifierFilter(CompareOp.EQUAL,
            new BinaryPrefixComparator(projectID));
        filters.add(filter);
      }
      // FIXME not working
      else if (projectID == null && fileID != null) {
        LOG.info("Filtering columns by fid.");
        Filter filter = new QualifierFilter(CompareOp.EQUAL,
            new BinaryRangeComparator(fileID, 16, false));
        filters.add(filter);
      }
      else if (projectID == null && fileID == null) {
        LOG.info("No column filtering."); 
      }
    }
    
    // Set a filter if required.
    if (!filters.isEmpty()) {
      Filter filterList = new FilterList(filters);
      scan.setFilter(filterList);
    }
    
    try {
      scanner = relationsInverseTable.getScanner(scan);
      
      boolean stop = false;
      List<RelationsGroupedModel> groups = 
          new ArrayList<RelationsGroupedModel>();
      
      for (Result result : scanner) {
        if (stop == true) {
          break;
        }
        
        groups = relationsInverseResultToRelationsGroupedModels(result);
        for (RelationsGroupedModel group : groups) {
          if (!appender.add(group)) {
            stop = true;
            break;
          }
        }
      }
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }
  
  public void retrieveRelationsFromFilesTableWithGet(
      ModelAppender<Model> appender, byte[] sourceID, Byte kind,
      byte[] targetID, byte[] projectID, byte[] fileID, Byte fileType)
  throws HBaseException {
    LOG.debug("*");
    File[] types;
    // 000
    if (fileType != null) {
      LOG.info("files table: Using Get operation");
      types = new File[] {
          (File) EnumUtil.getEnumByValue(File.values(), fileType) };
    }
    // 010
    else {
      LOG.info("files table: Using Get operation multiple times for each ft");
      types = File.values();
    }
    
    List<Row> batch = new ArrayList<Row>(types.length);
    Object[] results = new Object[types.length];
    Filter filter = null;
    
    // 011
    if (kind != null) {
      LOG.info("Filtering columns by rk.");
      WritableByteArrayComparable comp = new BinaryPrefixComparator(
          new byte[] {kind});
      filter = new QualifierFilter(CompareOp.EQUAL, comp);
    }
    // 111
    else {
      LOG.info("No column filtering.");
    }
    
    Get get = null;
    for (File crtType : types) {
      get = new Get(Bytes.add(projectID, new byte[] {crtType.getValue()},
          fileID));
      get.addFamily(FilesHBTable.CF_RELATIONS);
      if (filter != null) {
        get.setFilter(filter);
      }
      batch.add(get);
    }
    
    try {
      filesTable.batch(batch, results);
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    } catch (InterruptedException e) {}
    
    List<RelationsGroupedModel> groups = null;
    boolean stop = false;
    for (Object result : results) {
      if (stop) {
        break;
      }
      
      if (result != null && !((Result) result).isEmpty()) {
        groups = filesResultToRelationsGroupedModels((Result) result);
        for (RelationsGroupedModel group : groups) {
          if (!appender.add(group)) {
            stop = true;
            break;
          }
        }
      }
    }
    
  }
  
  public void retrieveRelationsFromFilesTable(ModelAppender<Model> appender,
      byte[] sourceID, Byte kind, byte[] targetID, byte[] projectID,
      byte[] fileID, Byte fileType)
  throws HBaseException {
    if (sourceID != null && targetID != null) {
      return;
    }
    
    Scan scan = null;
    byte[] startRow = null;
    List<Filter> filters = new ArrayList<Filter>(3);
    ResultScanner scanner = null;
    
    /*
     * Scanning / filtering by row key.
     */
    
    // 0x0
    if (projectID != null && fileID != null) {
      retrieveRelationsFromFilesTableWithGet(appender, sourceID, kind,
          targetID, projectID, fileID, fileType);
      return;
    }
    // 001
    else if (projectID != null && fileType != null && fileID == null) {
      LOG.info("files table: Scanning by pid ft");
      startRow = Bytes.add(projectID, new byte[] {fileType});
      scan = new Scan(startRow, Serialization.incBytes(startRow));
    }
    // 011
    else if (projectID != null && fileType == null && fileID == null) {
      LOG.info("files table: Scanning by pid");
      scan = new Scan(projectID, Serialization.incBytes(projectID));
    }
    else {
      scan = new Scan();
      
      // 100
      if (projectID == null && fileType != null && fileID != null) {
        LOG.info("files table: Scanning the whole table and filtering by ft fid");
        BinaryRangeComparator brc = new BinaryRangeComparator(
            Bytes.add(new byte[] {fileType}, fileID), 16, false);
        Filter filter = new RowFilter(CompareOp.EQUAL, brc);
        filters.add(filter);
      }
      // 101
      else if (projectID == null && fileType != null && fileID == null) {
        LOG.info("files table: Scanning the whole table and filtering by ft");
        BinaryRangeComparator brc = new BinaryRangeComparator(
            new byte[] {fileType}, 16, false);
        Filter filter = new RowFilter(CompareOp.EQUAL, brc);
        filters.add(filter);
      }
      // 110
      else if (projectID == null && fileType == null && fileID != null) {
        LOG.info("files table: Scanning the whole table and filtering by fid");
        BinaryRangeComparator brc = new BinaryRangeComparator(
            fileID, 17, false);
        Filter filter = new RowFilter(CompareOp.EQUAL, brc);
        filters.add(filter);
      }
      // 111
      else if (projectID == null && fileType == null && fileID == null) {
        LOG.info("files table: Scanning the whole table");
      }
    }
    
    /*
     * Filtering by column qualifier.
     */
    
    scan.addFamily(FilesHBTable.CF_RELATIONS);
    
    // 011
    if (kind != null) {
      LOG.info("Filtering columns by rk.");
      WritableByteArrayComparable comp = new BinaryPrefixComparator(
          new byte[] {kind});
      Filter filter = new QualifierFilter(CompareOp.EQUAL, comp);
      filters.add(filter);
    }
    // 111
    else {
      LOG.info("No column filtering.");
    }
    
    // Set a filter if required.
    if (!filters.isEmpty()) {
      Filter filterList = new FilterList(filters);
      scan.setFilter(filterList);
    }
    
    try {
      scanner = filesTable.getScanner(scan);
      
      boolean stop = false;
      List<RelationsGroupedModel> groups = null;
      
      for (Result result : scanner) {
        if (stop) {
          break;
        }
        
        groups = filesResultToRelationsGroupedModels(result);
        for (RelationsGroupedModel group : groups) {
          if (!appender.add(group)) {
            stop = true;
            break;
          }
        }
      }
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }
  
  public void retrieveRelations(ModelAppender<Model> appender, byte[] sourceID,
      Byte kind, byte[] targetID, byte[] projectID, byte[] fileID,
      Byte fileType)
  throws HBaseException {
    if (sourceID != null && targetID != null) {
      if (kind != null) {
        retrieveRelationsWithGet(appender, sourceID, kind,
              targetID, projectID, fileID);
      }
      else {
        Random rnd = new Random(System.currentTimeMillis());
        if (rnd.nextBoolean()) {
          retrieveRelationsFromRelationsDirectTable(appender, sourceID, kind,
              targetID, projectID, fileID);
        } else {
          // FIXME retrieve for inverse
          retrieveRelationsFromRelationsDirectTable(appender, sourceID, kind,
              targetID, projectID, fileID);
        }
      }
    }
    else if (sourceID == null && targetID == null) {
      retrieveRelationsFromFilesTable(appender, sourceID, kind,
          targetID, projectID, fileID, fileType);
    }
    else {
      if (sourceID != null && targetID == null) {
        retrieveRelationsFromRelationsDirectTable(appender, sourceID, kind,
            targetID, projectID, fileID);
      }
      else if (sourceID == null && targetID != null) {
        retrieveRelationsFromRelationsInverseTable(appender, sourceID, kind,
            targetID, projectID, fileID);
      }
      else {
        throw new Error("Programming error: This case was not treated.");
      }
    }
  }
  
  public void retrieveRelationsOld(ModelAppender<Model> appender, byte[] sourceID,
      Byte kind, byte[] targetID, byte[] projectID, byte[] fileID)
  throws HBaseException {
    if (sourceID == null || targetID == null || kind == null) {
      return;
    }
    
//    Get get = new Get(Bytes.add(sourceID, new byte[] {kind}, targetID));
    Get get = new Get(Bytes.add(targetID, new byte[] {kind}, sourceID));
    if (projectID != null && fileID != null) {
      get.addColumn(RelationsDirectHBTable.CF_DEFAULT,
          Bytes.add(projectID, fileID));
    }
    else {
      get.addFamily(RelationsDirectHBTable.CF_DEFAULT);
    }
    
    Result result = null;
    try {
//      result = relationsDirectTable.get(get);
      result = relationsInverseTable.get(get);
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    }
    
    if (result.isEmpty())
      LOG.info("No results found.");
    
//    List<RelationsGroupedModel> groups = getRelationsDirectFromResult(result);
    List<RelationsGroupedModel> groups = relationsInverseResultToRelationsGroupedModels(result);
    if (groups == null) {
      return;
    }
    for (RelationsGroupedModel group : groups) {
      appender.add(group);
    }
  }
  
  public void retrieveRelationsFromFilesOld(ModelAppender<Model> appender,
      byte[] projectID, Byte fileType, byte[] fileID, Byte kind,
      byte[] targetID, byte[] sourceID)
  throws HBaseException {
    if (projectID == null || fileType == null || fileID == null)
      return;
    
    Get get = new Get(Bytes.add(projectID, new byte[] {fileType}, fileID));
    if (kind != null && targetID != null & sourceID != null) {
      get.addColumn(FilesHBTable.CF_RELATIONS,
          Bytes.add(new byte[] {kind}, targetID, sourceID));
    }
    else {
      get.addFamily(FilesHBTable.CF_RELATIONS);
    }
    
    Result result = null;
    try {
      result = filesTable.get(get);
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    }
    
    if (result.isEmpty())
      LOG.info("No results found.");
    
    List<RelationsGroupedModel> groups = filesResultToRelationsGroupedModels(result);
    if (groups == null) {
      return;
    }
    for (RelationsGroupedModel group : groups) {
      appender.add(group);
    }
  }
  
  public void retrieveRelation(ModelAppender<Model> appender,
      byte[] relationID)
  throws HBaseException {
    if (relationID == null)
      return;
    
    Get get = new Get(relationID);
    get.addFamily(RelationsHashHBTable.CF_DEFAULT);
    
    Result result = null;
    try {
      result = relationsHashTable.get(get);
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    }
    
    RelationModel relation = relationsHashResultToRelationModel(result);
    appender.add(relation);
  }
  
  // TODO Other parameters -- search criteria
  public void retrieveRelations(ModelAppender<Model> appender,
      byte[] relationID)
  throws HBaseException {
    retrieveRelation(appender, relationID);
  }
}
