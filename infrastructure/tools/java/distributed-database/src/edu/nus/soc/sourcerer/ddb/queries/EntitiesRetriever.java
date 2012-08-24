package edu.nus.soc.sourcerer.ddb.queries;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHBTable;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;
import edu.nus.soc.sourcerer.ddb.tables.FilesHBTable;
import edu.nus.soc.sourcerer.ddb.util.BinaryRangeComparator;
import edu.nus.soc.sourcerer.ddb.util.ModelAppender;
import edu.nus.soc.sourcerer.model.ddb.EntitiesGroupedModel;
import edu.nus.soc.sourcerer.model.ddb.EntityExtraModel;
import edu.nus.soc.sourcerer.model.ddb.EntityModel;
import edu.nus.soc.sourcerer.model.ddb.Model;
import edu.nus.soc.sourcerer.model.ddb.SourcedRelationsModel;
import edu.nus.soc.sourcerer.model.ddb.SourcedRelationsModel.RelationTargetModel;
import edu.nus.soc.sourcerer.util.EnumUtil;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.File;

public class EntitiesRetriever {

  protected HTable entitiesHashTable;
  protected HTable entitiesTable;
  protected HTable filesTable;
  
  public EntitiesRetriever() throws HBaseException {
    super();
    
    // Get a table instance.
    try {
      entitiesHashTable = EntitiesHashHBTable.getInstance().getHTable();
      entitiesTable = EntitiesHBTable.getInstance().getHTable();
      filesTable = FilesHBTable.getInstance().getHTable();
    } catch (IOException e) {
      throw new HBaseConnectionException(
          "Could not connect to HBase for entities tables.", e);
    }
  }
  
  public static EntityModel entitiesHashResultToEntityModel(Result result) {
    if (result == null || result.isEmpty())
      return null;
    
    byte[] entityID = result.getRow();
    if (entityID == null)
      return null;
    
    byte[] type = result.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_ENTITYTYPE);
    byte[] fqn = result.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_FQN);
    byte[] projectID = result.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_PID);
    byte[] fileID = result.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_FID);
    byte[] modifiers = result.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_MODIFIERS);
    byte[] multi = result.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_MULTI);
    byte[] offset = result.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_OFFSET);
    byte[] length = result.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_LENGTH);
    byte[] loc = result.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_METRIC_LOC);
    byte[] nwloc = result.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_METRIC_NWLOC);
    
    EntityModel entity = new EntityModel();
    entity.setId(entityID);
    entity.setType(type == null ? Entity.UNKNOWN.getValue()
            : EnumUtil.getEnumByValue(Entity.values(), type[0]).getValue());
    entity.setFqn(fqn == null ? null : Bytes.toString(fqn));
    entity.setProjectID(projectID);
    entity.setFileID(fileID);
    entity.setModifiers(modifiers == null ? null : Bytes.toInt(modifiers));
    entity.setMulti(multi == null ? null : Bytes.toInt(multi));
    entity.setOffset(offset == null ? null : Bytes.toInt(offset));
    entity.setLength(length == null ? null : Bytes.toInt(length));
    entity.setLoc(loc == null ? null : Bytes.toInt(loc));
    entity.setNwloc(nwloc == null ? null : Bytes.toInt(nwloc));
    
    return entity;
  }
  
  public static List<EntitiesGroupedModel>
      entitiesHashResultToEntitiesGroupedModels(Result result) {
    if (result == null || result.isEmpty())
      return null;
    
    byte[] row = result.getRow();
    String fqn = Bytes.toString(Bytes.head(row, row.length - 33));
    byte[] projectID = new byte[16]; 
    System.arraycopy(row, row.length - 32, projectID, 0, 16);
    byte[] fileID = Bytes.tail(row, 16);
    
    return getEntitiesCols(fqn, projectID, fileID, result);
  }
  
  public static List<EntitiesGroupedModel> getEntitiesCols(String fqn,
      byte[] projectID, byte[] fileID, Result result) {
    NavigableMap<byte[], byte[]> map = result.getFamilyMap(
        EntitiesHBTable.CF_DEFAULT);
    Set<Entry<byte[], byte[]>> entrySet = map.entrySet();
    List<EntitiesGroupedModel> groups = new ArrayList<EntitiesGroupedModel>(
        map.size());
    EntitiesGroupedModel group = null;
    byte[] column = null, value = null;
    byte type = Entity.UNKNOWN.getValue();
    List<EntityExtraModel> extra = null;
    
    for (Entry<byte[], byte[]> entry : entrySet) {
      column = entry.getKey();
      if (column.length < 1) {
        continue;
      }
      type = column[0];
      
      value = entry.getValue();
      extra = EntityExtraModel.getExtraFromBytes(value);
      group = new EntitiesGroupedModel(fqn, projectID, fileID, type, extra);
      groups.add(group);
    }
    
    return groups;
  }
  
  public static List<EntitiesGroupedModel> filesResultToEntitiesGroupedModels(
      Result result) {
    if (result == null || result.isEmpty())
      return null;
    
    byte[] row = result.getRow();
    byte[] projectID = Bytes.head(row, 16);
    byte fileType = row[16];
    byte[] fileID = Bytes.tail(row, 16);
    
    return getFilesEntitiesCols(projectID, fileType, fileID, result);
  }
  
  public static List<EntitiesGroupedModel> getFilesEntitiesCols(
      byte[] projectID, Byte fileType, byte[] fileID, Result result) {
    NavigableMap<byte[], byte[]> map = result.getFamilyMap(
        FilesHBTable.CF_ENTITIES);
    Set<Entry<byte[], byte[]>> entrySet = map.entrySet();
    List<EntitiesGroupedModel> groups = new ArrayList<EntitiesGroupedModel>(
        map.size());
    EntitiesGroupedModel group = null;
    byte[] column = null, value = null;
    byte type = Entity.UNKNOWN.getValue();
    String fqn = null;
    List<EntityExtraModel> extra = null;
    
    for (Entry<byte[], byte[]> entry : entrySet) {
      column = entry.getKey();
      if (column.length < 1) {
        continue;
      }
      type = column[0];
      fqn = Bytes.toString(Bytes.tail(column, column.length - 1));
      
      value = entry.getValue();
      extra = EntityExtraModel.getExtraFromBytes(value);
      group = new EntitiesGroupedModel(fqn, projectID, fileID, type, extra);
      group.setFileType(fileType);
      groups.add(group);
    }
    
    return groups;
  }
  
  public static SourcedRelationsModel entitiesHashResultToSourcedRelationsModel(
      Result result) {
    if (result == null || result.isEmpty())
      return null;
    
    byte[] sourceID = result.getRow();
    if (sourceID == null)
      return null;
    
    byte[] sourceType = result.getValue(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_TYPE);
    byte[] rank = result.getValue(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
    byte[] bRelationsCount = result.getValue(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_TARGETS_COUNT);
    byte[] targets = result.getValue(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_TARGETS);
    byte[] relationIDs = result.getValue(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS);
    
    // Create the model.
    SourcedRelationsModel sourcedRelation = null;
    if (bRelationsCount == null
        || (bRelationsCount != null && Bytes.toInt(bRelationsCount) == 0)) {
      sourcedRelation = new SourcedRelationsModel(sourceID,
          sourceType == null ? Entity.UNKNOWN.getValue() : sourceType[0],
          rank == null ? null : Bytes.toDouble(rank));
    }
    else {
      int relationsCount = Bytes.toInt(bRelationsCount); 
      sourcedRelation = new SourcedRelationsModel(sourceID,
          sourceType == null ? Entity.UNKNOWN.getValue() : sourceType[0],
          rank == null ? null : Bytes.toDouble(rank),
          relationsCount);

      // DEBUG
//      if (targets.length != relationsCount * 18)
//        System.err.println("** " + Serialization.byteArrayToHexString(result.getRow()) + ": "
//            + "wrong targets size " + targets.length + " when count is " + relationsCount);
//      if (relationIDs.length != relationsCount * 16)
//        System.err.println("** " + Serialization.byteArrayToHexString(result.getRow()) + ": "
//            + "wrong relationIDs size " + relationIDs.length + " when count is " + relationsCount);
      
      // Read from array bytes information about relations and targets
      // and add them to the model.
      ByteBuffer bbTargets = null;
      if (targets != null)
        bbTargets = ByteBuffer.wrap(targets);
      ByteBuffer bbRelationIDs = null;
      if (relationIDs != null)
        bbRelationIDs = ByteBuffer.wrap(relationIDs);
      byte[] targetID = null;
      byte[] targetType = null;
      byte[] relationID = null;
      byte[] relationKind = null;
      RelationTargetModel relationTarget = null;
      for (int i = 0; i < relationsCount; i++) {
        if (targets != null) {
          targetID = new byte[16];
          targetType = new byte[1];
          relationKind = new byte[1];
        }
        if (relationIDs != null) {
          relationID = new byte[16];
        }
        
        try {
          if (targets != null) {
            bbTargets.get(relationKind, 0, 1);
            bbTargets.get(targetType, 0, 1);
            bbTargets.get(targetID, 0, 16);
          }
          if (relationIDs != null) {
            bbRelationIDs.get(relationID, 0, 16);
          }
        } catch (BufferUnderflowException e) {
          e.printStackTrace();
          break;
        }
        
        relationTarget = new RelationTargetModel(targetID,
            targetType != null ? targetType[0] : null,
            relationID,
            relationKind != null ? relationKind[0] : null);
        sourcedRelation.addRelation(relationTarget);
      }
    }
    
    return sourcedRelation;
  }
  
  public void retrieveEntity(ModelAppender<Model> appender, byte[] entityID,
      boolean sourcedRelations) throws HBaseException {
    if (entityID == null)
      return;
    
    Get get = new Get(entityID);
    if (!sourcedRelations) {
      get.addFamily(EntitiesHashHBTable.CF_DEFAULT);
      get.addFamily(EntitiesHashHBTable.CF_METRICS);
    }
    else {
      get.addFamily(EntitiesHashHBTable.CF_RELATIONS);
    }
    
    Result result = null;
    try {
      result = entitiesHashTable.get(get);
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    }
    
    Model model = null;
    if (!sourcedRelations) {
      model = entitiesHashResultToEntityModel(result);
    }
    else {
      model = entitiesHashResultToSourcedRelationsModel(result);
    }
    appender.add(model);
  }
  
  public void retrieveEntity(ModelAppender<Model> appender, byte[] entityID)
      throws HBaseException {
    retrieveEntity(appender, entityID, false);
  }
  
  public void retrieveEntitiesFromEntitiesTableWithGet(
      ModelAppender<Model> appender, String fqn, byte[] projectID,
      byte[] fileID, Byte type)
  throws HBaseException {
    Get get = new Get(Bytes.add(Bytes.toBytes(fqn), projectID, fileID));
    if (type != null) {
      get.addColumn(EntitiesHBTable.CF_DEFAULT, new byte[] {type});
    }
    else {
      get.addFamily(EntitiesHBTable.CF_DEFAULT);
    }
    
    Result result = null;
    try {
      result = entitiesTable.get(get);
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    }
    
    if (result.isEmpty())
      LOG.info("No results found.");
    
    List<EntitiesGroupedModel> groups = 
        entitiesHashResultToEntitiesGroupedModels(result);
    if (groups == null) {
      return;
    }
    for (EntitiesGroupedModel group : groups) {
      if (!appender.add(group)) {
        break;
      }
    }
  }
  
  public void retrieveEntitiesFromEntitiesTable(ModelAppender<Model> appender,
      String fqn, byte[] projectID, byte[] fileID, Byte type)
  throws HBaseException {
    if (fqn == null) {
      return;
    }
    
    // 00
    if (projectID != null && fileID != null
        && fqn.endsWith("\0")) {
      retrieveEntitiesFromEntitiesTableWithGet(appender, fqn, projectID, fileID, type);
      return;
    }
    
    Scan scan = null;
    byte[] startRow = null;
    Filter filter = null;
    ResultScanner scanner = null;
    
    // Scan by FQN prefix.
    if (!fqn.endsWith("\0")) {
      startRow = Bytes.toBytes(fqn);
      scan = new Scan(startRow, Serialization.incBytes(startRow));
      
      // 00
      if (projectID != null && fileID != null) {
        LOG.info("Scanning by fqn-prefix and filtering by pid fid");
        BinaryRangeComparator brc = new BinaryRangeComparator(
            Bytes.add(projectID, fileID), 32, true);
        filter = new RowFilter(CompareOp.EQUAL, brc);
      }
      // 01
      else if (projectID != null && fileID == null) {
        LOG.info("Scanning by fqn-prefix and filtering by pid");
        BinaryRangeComparator brc = new BinaryRangeComparator(
            projectID, 32, true);
        filter = new RowFilter(CompareOp.EQUAL, brc);
      }
      // 10
      else if (projectID == null && fileID != null) {
        LOG.info("Scanning by fqn-prefix and filtering by fid");
        BinaryRangeComparator brc = new BinaryRangeComparator(
            fileID, 16, true);
        filter = new RowFilter(CompareOp.EQUAL, brc);
      }
      // 11
      else if (projectID == null && fileID == null) {
        LOG.info("Scanning by fqn-prefix");
      }
    }
    // Scan by exact FQN.
    else {
      // 01
      if (projectID != null && fileID == null) {
        LOG.info("Scanning by fqn pid");
        startRow = Bytes.add(Bytes.toBytes(fqn), projectID);
        scan = new Scan(startRow, Serialization.incBytes(startRow));
      }
      // 1x
      else {
        startRow = Bytes.toBytes(fqn);
        scan = new Scan(startRow, Serialization.incBytes(startRow));
        
        // 10
        if (projectID == null && fileID != null) {
          LOG.info("Scanning by fqn and filtering by fid");
          BinaryRangeComparator brc = new BinaryRangeComparator(
              fileID, 16, true);
          filter = new RowFilter(CompareOp.EQUAL, brc);
        }
        // 11
        else if (projectID == null && fileID == null) {
          LOG.info("Scanning by fqn");
        }
      }
    }
    
    // Set filter if required.
    if (filter != null) {
      scan.setFilter(filter);
    }
    
    // Select by type if necessary.
    if (type != null) {
      scan.addColumn(EntitiesHBTable.CF_DEFAULT, new byte[] {type});
    }
    else {
      scan.addFamily(EntitiesHBTable.CF_DEFAULT);
    }
    
    try {
      scanner = entitiesTable.getScanner(scan);
      
      boolean stop = false;
      List<EntitiesGroupedModel> groups = null;
      for (Result result : scanner) {
        if (stop) {
          break;
        }
        
        groups = entitiesHashResultToEntitiesGroupedModels(result);
        if (groups == null) {
          return;
        }
        for (EntitiesGroupedModel group : groups) {
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
  
  public void retrieveEntitiesFromFilesTableWithGet(
      ModelAppender<Model> appender, String fqn, byte[] projectID,
      byte[] fileID, Byte fileType, Byte type)
  throws HBaseException {
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
    if (type != null) {
      LOG.info("Filtering columns by entity type.");
      filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
          new BinaryPrefixComparator(new byte[] {type}));
    }
    
    Get get = null;
    for (File crtType : types) {
      get = new Get(Bytes.add(projectID, new byte[] {crtType.getValue()},
          fileID));
      get.addFamily(FilesHBTable.CF_ENTITIES);
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
    
    List<EntitiesGroupedModel> groups = null;
    boolean stop = false;
    for (Object result : results) {
      if (stop == true) {
        break;
      }
      
      if (result != null && !((Result) result).isEmpty()) {
        groups = filesResultToEntitiesGroupedModels((Result) result);
        for (EntitiesGroupedModel group : groups) {
          if (!appender.add(group)) {
            stop = true;
            break;
          }
        }
      }
    }
    
  }
  
  public void retrieveEntitiesFromFilesTable(ModelAppender<Model> appender,
      String fqn, byte[] projectID, byte[] fileID, Byte fileType, Byte type)
  throws HBaseException {
    if (fqn != null) {
      return;
    }
    
    Scan scan = null;
    byte[] startRow = null;
    List<Filter> filters = new ArrayList<Filter>(2);
    ResultScanner scanner = null;
    
    // 0x0
    if (projectID != null && fileID != null) {
      retrieveEntitiesFromFilesTableWithGet(appender, fqn, projectID, fileID,
          fileType, type);
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
    
    // Filter by entity type if required.
    if (type != null) {
      LOG.info("files table: Filtering columns by entity type.");
      Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
          new BinaryPrefixComparator(new byte[] {type}));
      filters.add(filter);
    }
    
    // Set a filter if required.
    if (!filters.isEmpty()) {
      Filter filterList = new FilterList(filters);
      scan.setFilter(filterList);
    }
    
    try {
      scanner = filesTable.getScanner(scan);
      
      boolean stop = false;
      List<EntitiesGroupedModel> groups = null;
      
      for (Result result : scanner) {
        if (stop == true) {
          break;
        }
        
        groups = filesResultToEntitiesGroupedModels(result);
        for (EntitiesGroupedModel group : groups) {
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
  
  public void retrieveEntities(ModelAppender<Model> appender, String fqn,
      byte[] projectID, byte[] fileID, Byte fileType, Byte type)
  throws HBaseException {
    if (fqn != null && projectID != null
        || fqn != null && projectID == null) {
      retrieveEntitiesFromEntitiesTable(appender, fqn, projectID, fileID, type);
    }
    else if (fqn == null) {
      retrieveEntitiesFromFilesTable(appender, fqn, projectID, fileID,
          fileType, type);
    }
  }
  
  public double retrieveCodeRank(byte[] entityID) throws HBaseException {
    if (entityID == null)
      throw new IllegalArgumentException("Entity ID must not be null.");
    
    Get get = new Get(entityID);
    get.addColumn(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
    
    Result result = null;
    try {
      result = entitiesHashTable.get(get);
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    }
    
    byte[] bRank = result.getValue(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
    
    if (bRank == null)
      return Double.MAX_VALUE;
    
    return Bytes.toDouble(bRank);
  }
}
