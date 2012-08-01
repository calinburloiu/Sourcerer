package edu.nus.soc.sourcerer.ddb.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.DirectRelationsHBTable;
import edu.nus.soc.sourcerer.ddb.tables.FilesHBTable;
import edu.nus.soc.sourcerer.ddb.tables.InverseRelationsHBTable;
import edu.nus.soc.sourcerer.model.ddb.RelationModel;

public class RelationModelInserter implements ModelInserter<RelationModel> {
  protected int selectRowsCount;
  protected HTable inverseRelationsTable;
  protected HTable directRelationsTable;
  protected HTable filesTable;
  
  public RelationModelInserter(int selectRowsCount)
      throws HBaseConnectionException {
    super();
    this.selectRowsCount = selectRowsCount;

    // Get a table instance.
    try {
      inverseRelationsTable = InverseRelationsHBTable.getInstance().getHTable();
      directRelationsTable = DirectRelationsHBTable.getInstance().getHTable();
      filesTable = FilesHBTable.getInstance().getHTable();
    } catch (IOException e) {
      throw new HBaseConnectionException(
          "Could not connect to HBase.", e);
    }
    inverseRelationsTable.setAutoFlush(false);
    directRelationsTable.setAutoFlush(false);
    filesTable.setAutoFlush(false);
  }
  
  @Override
  public void insertModels(Collection<RelationModel> models) throws HBaseException {
    ArrayList<Put> inverseRelationPuts = new ArrayList<Put>(selectRowsCount);
    ArrayList<Put> directRelationPuts = new ArrayList<Put>(selectRowsCount);
    ArrayList<Put> filePuts = new ArrayList<Put>(selectRowsCount);
    Put inverseRelationPut = null;
    Put directRelationPut = null;
    Put filePut = null;
    
    for (RelationModel relation : models) {
      /*try {*/
        inverseRelationPut = new Put(Bytes.add(relation.getTargetID(),
            new byte[] {relation.getRelationKind()},
            relation.getSourceID()));
        directRelationPut = new Put(Bytes.add(relation.getSourceID(),
            new byte[] {relation.getRelationKind()},
            relation.getTargetID()));
        filePut = new Put(Bytes.add(relation.getProjectID(),
            new byte[] {relation.getFileType()}, relation.getFileID()));
        
        // * `inverse_relations` TABLE
        // TODO Add offset and length to inverse_relations table.
        inverseRelationPut.add(InverseRelationsHBTable.CF_DEFAULT,
            Bytes.add(relation.getProjectID(), relation.getFileID()),
            new byte[] {});
        
        // * `direct_relations` TABLE
        // TODO Add (maybe) offset and length to inverse_relations table.
        directRelationPut.add(DirectRelationsHBTable.CF_DEFAULT,
            Bytes.add(relation.getProjectID(), relation.getFileID()),
            new byte[] {});
        
        // * `files` TABLE
        filePut.add(FilesHBTable.CF_RELATIONS,
            Bytes.add(new byte[] {relation.getRelationKind()},
                relation.getTargetID(), relation.getSourceID()),
            new byte[] {});
       
        inverseRelationPuts.add(inverseRelationPut);
        directRelationPuts.add(directRelationPut);
        filePuts.add(filePut);
      /*}  catch (UnsupportedEncodingException e) {
        throw new StringSerializationException(e);
      }*/
    }
    
    try {
      inverseRelationsTable.put(inverseRelationPuts);
      inverseRelationsTable.flushCommits();
      
      directRelationsTable.put(directRelationPuts);
      directRelationsTable.flushCommits();
      
      filesTable.put(filePuts);
      filesTable.flushCommits();
    } catch (IOException e) {
      throw new HBaseException(e);
    }
  }

}
