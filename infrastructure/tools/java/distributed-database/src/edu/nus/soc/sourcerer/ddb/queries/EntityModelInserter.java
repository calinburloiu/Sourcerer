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
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHBTable;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;
import edu.nus.soc.sourcerer.ddb.tables.FilesHBTable;
import edu.nus.soc.sourcerer.model.ddb.EntityModel;
import edu.nus.soc.sourcerer.util.StringSerializationException;

public class EntityModelInserter implements ModelInserter<EntityModel> {
  protected int selectRowsCount;
  protected HTable entitiesTable;
  protected HTable entitiesHashTable;
  protected HTable filesTable;
  
  public EntityModelInserter(int selectRowsCount)
      throws HBaseConnectionException {
    super();
    this.selectRowsCount = selectRowsCount;

    // Get a table instance.
    try {
      entitiesTable = EntitiesHBTable.getInstance().getHTable();
      entitiesHashTable = EntitiesHashHBTable.getInstance().getHTable();
      filesTable = FilesHBTable.getInstance().getHTable();
    } catch (IOException e) {
      throw new HBaseConnectionException(
          "Could not connect to HBase.", e);
    }
    entitiesTable.setAutoFlush(false);
    entitiesHashTable.setAutoFlush(false);
    filesTable.setAutoFlush(false);
  }
  
  @Override
  public void insertModels(Collection<EntityModel> models) throws HBaseException {
    ArrayList<Put> entityPuts = new ArrayList<Put>(selectRowsCount);
    ArrayList<Put> entityHashPuts = new ArrayList<Put>(selectRowsCount);
    ArrayList<Put> filePuts = new ArrayList<Put>(selectRowsCount);
    Put entityPut = null;
    Put entityHashPut = null;
    Put filePut = null;
    
    // Add each entity to HBase.
    for (EntityModel entity : models) {
      try {
        entityPut = new Put(Bytes.add(
            (entity.getFqn() + '\0').getBytes("UTF-8"), entity.getProjectID(),
            entity.getFileID()));
        entityHashPut = new Put(entity.getId());
        filePut = new Put(Bytes.add(entity.getProjectID(), 
            new byte[] {entity.getFileType().getValue()}, entity.getFileID()));
        
        // * `entities` TABLE
        // TODO Add modifiers, multi, offset and length to entities table.
        entityPut.add(EntitiesHBTable.CF_DEFAULT,
            new byte[] {entity.getType().getValue()},
            new byte[] {});
        
        // * `entities_hash` TABLE
        if (entity.getType() != null) {
          entityHashPut.add(EntitiesHashHBTable.CF_DEFAULT,
              EntitiesHashHBTable.COL_ENTITYTYPE,
              new byte[] {entity.getType().getValue()});
        }
        entityHashPut.add(EntitiesHashHBTable.CF_DEFAULT,
            EntitiesHashHBTable.COL_FQN,
            entity.getFqn().getBytes("UTF-8"));
        entityHashPut.add(EntitiesHashHBTable.CF_DEFAULT,
            EntitiesHashHBTable.COL_PID,
            entity.getProjectID());
        if (entity.getFileID() != null) {
          entityHashPut.add(EntitiesHashHBTable.CF_DEFAULT,
              EntitiesHashHBTable.COL_FID,
              entity.getFileID());
        }
        if (entity.getModifiers() != null) {
          entityHashPut.add(EntitiesHashHBTable.CF_DEFAULT,
              EntitiesHashHBTable.COL_MODIFIERS,
              Bytes.toBytes(entity.getModifiers()));
        }
        if (entity.getMulti() != null) {
          entityHashPut.add(EntitiesHashHBTable.CF_DEFAULT,
              EntitiesHashHBTable.COL_MULTI,
              Bytes.toBytes(entity.getMulti()));
        }
        if (entity.getOffset() != null) {
          entityHashPut.add(EntitiesHashHBTable.CF_DEFAULT,
              EntitiesHashHBTable.COL_OFFSET,
              Bytes.toBytes(entity.getOffset()));
        }
        if (entity.getLength() != null) {
          entityHashPut.add(EntitiesHashHBTable.CF_DEFAULT,
              EntitiesHashHBTable.COL_LENGTH,
              Bytes.toBytes(entity.getLength()));
        }
        if (entity.getLoc() != null) {
          entityHashPut.add(EntitiesHashHBTable.CF_METRICS,
              EntitiesHashHBTable.COL_METRIC_LOC,
              Bytes.toBytes(entity.getLoc()));
        }
        if (entity.getNwloc() != null) {
          entityHashPut.add(EntitiesHashHBTable.CF_METRICS,
              EntitiesHashHBTable.COL_METRIC_NWLOC,
              Bytes.toBytes(entity.getNwloc()));
        }
        
        // * `files` TABLE
        filePut.add(FilesHBTable.CF_ENTITIES, 
            Bytes.add(new byte[] {entity.getType().getValue()}, 
                entity.getFqn().getBytes("UTF-8")),
            new byte[] {});
        
        entityPuts.add(entityPut);
        entityHashPuts.add(entityHashPut);
        filePuts.add(filePut);
      } catch (UnsupportedEncodingException e) {
        throw new StringSerializationException(e);
      }
    }
    
    try {
      entitiesTable.put(entityPuts);
      entitiesTable.flushCommits();
      entitiesHashTable.put(entityHashPuts);
      entitiesHashTable.flushCommits();
      filesTable.put(filePuts);
      filesTable.flushCommits();
    } catch (IOException e) {
      throw new HBaseException(e);
    }
  }

}
