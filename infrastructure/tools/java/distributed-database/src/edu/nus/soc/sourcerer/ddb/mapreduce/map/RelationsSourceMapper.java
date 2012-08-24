package edu.nus.soc.sourcerer.ddb.mapreduce.map;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationSourceWritable;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationTargetsWritable;
import edu.nus.soc.sourcerer.ddb.tables.RelationsHashHBTable;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

/**
 * Maps `relations_hash` HBase table to keys with the source of relations
 * (ID and type) and values that describe relation (ID and kind) and target
 * (ID and type).
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class RelationsSourceMapper
    extends TableMapper<RelationSourceWritable, RelationTargetsWritable> {

  public enum Counters { ROWS, ERRORS };
  
  @Override
  protected void map(ImmutableBytesWritable row, Result columns, Context context)
      throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    // Output key
    byte[] bSourceType = columns.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_SOURCE_TYPE);
    byte[] bSourceID = columns.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_SOURCE_ID);
    RelationSourceWritable outKey = new RelationSourceWritable(
        bSourceType == null ? Entity.UNKNOWN.getValue() : bSourceType[0],
        bSourceID);
    
    // Output value
    byte[] bKind = columns.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_KIND);
    byte[] bTargetType = columns.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_TARGET_TYPE);
    byte[] bTargetID = columns.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_TARGET_ID);
    byte[] bRelationID = row.get();
    RelationTargetsWritable outVal = new RelationTargetsWritable(
        bKind == null ?
            (byte) (Relation.UNKNOWN.getValue() | RelationClass.UNKNOWN.getValue())
            : bKind[0],
        bTargetType == null ? Entity.UNKNOWN.getValue() : bTargetType[0],
        bTargetID, bRelationID);
    
    try {
      // Write output.
      context.write(outKey, outVal);
    } catch (IOException e) {
      LOG.error("Mapper could not write row " + Bytes.toStringBinary(row.get()));
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }
}
