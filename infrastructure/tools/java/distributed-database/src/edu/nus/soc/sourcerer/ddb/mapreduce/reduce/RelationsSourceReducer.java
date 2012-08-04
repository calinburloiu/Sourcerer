package edu.nus.soc.sourcerer.ddb.mapreduce.reduce;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationSourceWritable;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationTargetsWritable;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;
import edu.nus.soc.sourcerer.util.Serialization;

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

    int targetsCount = 0;
    ByteBuffer targetsBuf = ByteBuffer.allocate(4096);
    ByteBuffer relationsBuf = ByteBuffer.allocate(4096);
    for (RelationTargetsWritable target : targets) {
      targetsCount++;
      
      do {
        try {
          writeTargetEntityBytes(targetsBuf, target);
          break;
        } catch (BufferOverflowException e) {
          targetsBuf = Serialization.reallocateByteBuffer(targetsBuf);
        }
      } while (true);
      
      do {
        try {
          writeRelationIDBytes(relationsBuf, target);
          break;
        } catch (BufferOverflowException e) {
          relationsBuf = Serialization.reallocateByteBuffer(relationsBuf);
        }
      } while (true);
    }
    
    // `te` (Target Entities)
    put.add(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_TARGETS,
        Serialization.getFitByteBufferBytes(targetsBuf));
    
    // `rids` (Relation IDs)
    put.add(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS,
        Serialization.getFitByteBufferBytes(relationsBuf));
    
    // Initial value for `rank` (source CodeRank) is 1 / targetsCount
    double rank = 1.0d / targetsCount;
    put.add(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK,
        Bytes.toBytes(rank));
    
    // `tec` (Target Entities Count)
    put.add(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_TARGETS_COUNT,
        Bytes.toBytes(targetsCount));
    
    try {
      // Write output.
      context.write(new ImmutableBytesWritable(source.getSourceID()), put);
      context.getCounter(Counters.VALID).increment(1);
    } catch (IOException e) {
      LOG.error("Reducer could not write row "
          + Bytes.toStringBinary(source.getSourceID()));
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }
  
  public static void writeTargetEntityBytes(ByteBuffer buf,
      RelationTargetsWritable target) {
    buf.put(target.getKind());
    buf.put(target.getTargetType());
    buf.put(target.getTargetID());
  }
  
  public static void writeRelationIDBytes(ByteBuffer buf,
      RelationTargetsWritable target) {
    buf.put(target.getRelationID());
  }
  
}
