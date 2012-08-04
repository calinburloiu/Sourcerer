package edu.nus.soc.sourcerer.ddb.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
//import edu.nus.soc.sourcerer.ddb.tables.RelationsDirectHBTable;
//import edu.nus.soc.sourcerer.ddb.tables.FilesHBTable;
import edu.nus.soc.sourcerer.ddb.tables.RelationsHashHBTable;
//import edu.nus.soc.sourcerer.ddb.tables.RelationsInverseHBTable;
import edu.nus.soc.sourcerer.model.ddb.RelationModel;


public class RelationModelInserter implements ModelInserter<RelationModel> {
  protected int selectRowsCount;
  protected HTable relationsHashTable;
//  protected HTable inverseRelationsTable;
//  protected HTable directRelationsTable;
//  protected HTable filesTable;
  
  public RelationModelInserter(int selectRowsCount)
      throws HBaseConnectionException {
    super();
    this.selectRowsCount = selectRowsCount;

    // Get a table instance.
    try {
      relationsHashTable = RelationsHashHBTable.getInstance().getHTable();
//      inverseRelationsTable = RelationsInverseHBTable.getInstance().getHTable();
//      directRelationsTable = RelationsDirectHBTable.getInstance().getHTable();
//      filesTable = FilesHBTable.getInstance().getHTable();
    } catch (IOException e) {
      throw new HBaseConnectionException(
          "Could not connect to HBase.", e);
    }
    relationsHashTable.setAutoFlush(false);
//    inverseRelationsTable.setAutoFlush(false);
//    directRelationsTable.setAutoFlush(false);
//    filesTable.setAutoFlush(false);
  }
  
  @Override
  public void insertModels(Collection<RelationModel> models) throws HBaseException {
    ArrayList<Put> relationHashPuts = new ArrayList<Put>(selectRowsCount);
//    ArrayList<Put> inverseRelationPuts = new ArrayList<Put>(selectRowsCount);
//    ArrayList<Put> directRelationPuts = new ArrayList<Put>(selectRowsCount);
//    ArrayList<Put> filePuts = new ArrayList<Put>(selectRowsCount);
    Put relationHashPut = null;
//    Put inverseRelationPut = null;
//    Put directRelationPut = null;
//    Put filePut = null;
    
    for (RelationModel relation : models) {
      /*try {*/
        relationHashPut = new Put(relation.getId());
//        inverseRelationPut = new Put(Bytes.add(relation.getTargetID(),
//            new byte[] {relation.getKind()},
//            relation.getSourceID()));
//        directRelationPut = new Put(Bytes.add(relation.getSourceID(),
//            new byte[] {relation.getKind()},
//            relation.getTargetID()));
//        filePut = new Put(Bytes.add(relation.getProjectID(),
//            new byte[] {relation.getFileType()}, relation.getFileID()));
        
        relationHashPut.add(RelationsHashHBTable.CF_DEFAULT,
            RelationsHashHBTable.COL_KIND,
            new byte[] {relation.getKind()});
        if (relation.getSourceID() != null) {
          relationHashPut.add(RelationsHashHBTable.CF_DEFAULT,
              RelationsHashHBTable.COL_SOURCE_ID,
              relation.getSourceID());
        }
        if (relation.getSourceType() != null) {
          relationHashPut.add(RelationsHashHBTable.CF_DEFAULT,
              RelationsHashHBTable.COL_SOURCE_TYPE,
              new byte[] {relation.getSourceType()});
        }
        if (relation.getTargetID() != null) {
          relationHashPut.add(RelationsHashHBTable.CF_DEFAULT,
              RelationsHashHBTable.COL_TARGET_ID,
              relation.getTargetID());
        }
        if (relation.getTargetType() != null) {
          relationHashPut.add(RelationsHashHBTable.CF_DEFAULT,
              RelationsHashHBTable.COL_TARGET_TYPE,
              new byte[] {relation.getTargetType()});
        }
        if (relation.getProjectID() != null) {
          relationHashPut.add(RelationsHashHBTable.CF_DEFAULT,
              RelationsHashHBTable.COL_PROJECT_ID,
              relation.getProjectID());
        }
        if (relation.getFileID() != null) {
          relationHashPut.add(RelationsHashHBTable.CF_DEFAULT,
              RelationsHashHBTable.COL_FILE_ID,
              relation.getFileID());
        }
        if (relation.getFileType() != null) {
          relationHashPut.add(RelationsHashHBTable.CF_DEFAULT,
              RelationsHashHBTable.COL_FILE_TYPE,
              new byte[] {relation.getFileType()});
        }
        if (relation.getOffset() != null) {
          relationHashPut.add(RelationsHashHBTable.CF_DEFAULT,
              RelationsHashHBTable.COL_OFFSET,
              Bytes.toBytes(relation.getOffset()));
        }
        if (relation.getLength() != null) {
          relationHashPut.add(RelationsHashHBTable.CF_DEFAULT,
              RelationsHashHBTable.COL_LENGTH,
              Bytes.toBytes(relation.getLength()));
        }
        
//        // * `inverse_relations` TABLE
//        // TODO Add offset and length to inverse_relations table.
//        inverseRelationPut.add(RelationsInverseHBTable.CF_DEFAULT,
//            Bytes.add(relation.getProjectID(), relation.getFileID()),
//            new byte[] {});
//        
//        // * `direct_relations` TABLE
//        // TODO Add (maybe) offset and length to inverse_relations table.
//        directRelationPut.add(RelationsDirectHBTable.CF_DEFAULT,
//            Bytes.add(relation.getProjectID(), relation.getFileID()),
//            new byte[] {});
//        
//        // * `files` TABLE
//        filePut.add(FilesHBTable.CF_RELATIONS,
//            Bytes.add(new byte[] {relation.getKind()},
//                relation.getTargetID(), relation.getSourceID()),
//            new byte[] {});
       
        relationHashPuts.add(relationHashPut);
//        inverseRelationPuts.add(inverseRelationPut);
//        directRelationPuts.add(directRelationPut);
//        filePuts.add(filePut);
      /*}  catch (UnsupportedEncodingException e) {
        throw new StringSerializationException(e);
      }*/
    }
    
    try {
      relationsHashTable.put(relationHashPuts);
      relationsHashTable.flushCommits();
      
//      inverseRelationsTable.put(inverseRelationPuts);
//      inverseRelationsTable.flushCommits();
//      
//      directRelationsTable.put(directRelationPuts);
//      directRelationsTable.flushCommits();
//      
//      filesTable.put(filePuts);
//      filesTable.flushCommits();
    } catch (IOException e) {
      throw new HBaseException(e);
    }
  }

}
