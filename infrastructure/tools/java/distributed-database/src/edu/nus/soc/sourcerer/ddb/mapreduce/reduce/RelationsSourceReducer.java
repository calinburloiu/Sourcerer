package edu.nus.soc.sourcerer.ddb.mapreduce.reduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationSourceWritable;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationTargetsWritable;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationTargetsWritable.RelationTargetsSerialization;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

public class RelationsSourceReducer
    extends TableReducer<RelationSourceWritable, RelationTargetsWritable,
        ImmutableBytesWritable> {

  public enum Counters { ROWS, ERRORS, VALID };
  
  @Override
  protected void reduce(RelationSourceWritable source,
      Iterable<RelationTargetsWritable> targets, Context context)
          throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    Put put = new Put(source.getSourceID());
    
    // `set` (Source Entity Type)
    put.add(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_TYPE,
        new byte[] {source.getSourceType()});
    
    RelationTargetsSerialization rts = new RelationTargetsSerialization(4096);
    rts.write(targets);
    int targetsCount = rts.getCount();
    byte[] targetsBytes = rts.getTargetsBytes();
    byte[] relationsBytes = rts.getRelationsBytes();
    
    // `tec` (Target Entities Count)
    put.add(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_TARGETS_COUNT,
        Bytes.toBytes(targetsCount));
    
    // `te` (Target Entities)
    put.add(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_TARGETS, targetsBytes);
    
    // `rids` (Relation IDs)
    put.add(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS, relationsBytes);
    
    // Initial value for `rank` (source CodeRank) is 1 / entitiesCount
//    long entitiesCount = context.getConfiguration().getLong(
//        ConfigurationParams.ENTITIES_COUNT, 0);
//    double rank = 1.0d / entitiesCount;
//    put.add(EntitiesHashHBTable.CF_RELATIONS,
//        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK,
//        Bytes.toBytes(rank));
    try {
      // Write output.
      context.write(new ImmutableBytesWritable(source.getSourceID()), put);
      context.getCounter(Counters.VALID).increment(1);
    } catch (IOException e) {
      System.err.println("Reducer could not write row "
          + Bytes.toStringBinary(source.getSourceID()));
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }
}
