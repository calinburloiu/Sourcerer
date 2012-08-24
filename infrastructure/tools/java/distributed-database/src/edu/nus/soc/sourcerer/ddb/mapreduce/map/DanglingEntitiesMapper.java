package edu.nus.soc.sourcerer.ddb.mapreduce.map;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;

import edu.nus.soc.sourcerer.ddb.mapreduce.ConfigurationParams;
import edu.nus.soc.sourcerer.ddb.mapreduce.Util;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

@Deprecated
public class DanglingEntitiesMapper 
    extends TableMapper<ByteWritable, DoubleWritable> {

  protected long entitiesCount;
  
  public enum Counters {RUNS, MAPS, ERRORS, ROWS, DUMMY,
    DANGLING_ENTITIES, UNRANKED_DANGLING_ENTITIES};
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    entitiesCount = context.getConfiguration().getLong(
        ConfigurationParams.ENTITIES_COUNT, 0);
  }

  @Override
  protected void map(ImmutableBytesWritable dummyKey, Result dummyValue,
      Context context) throws IOException, InterruptedException {
    context.getCounter(Counters.MAPS).increment(1);
    
    Result value = null;
    double sum = 0.0d;
    byte[] bRank;
    byte[] bTargetsCount;
    
    try {
      while (context.nextKeyValue()) {
        context.getCounter(Counters.ROWS).increment(1);
        
        value = context.getCurrentValue();
        
        bRank = value.getValue(EntitiesHashHBTable.CF_RELATIONS,
            EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
        bTargetsCount = value.getValue(EntitiesHashHBTable.CF_RELATIONS,
            EntitiesHashHBTable.COL_RELATIONS_TARGETS_COUNT);
        
        if (bTargetsCount == null
            || (bTargetsCount != null && Bytes.toInt(bTargetsCount) == 0)) {
          context.getCounter(Counters.DANGLING_ENTITIES).increment(1);
          if (bRank == null) {
            context.getCounter(
                Counters.UNRANKED_DANGLING_ENTITIES).increment(1);
            sum += 1.0d / entitiesCount;
          }
          else {
            sum += Bytes.toDouble(bRank);
          }
        }
      }
      
      context.write(new ByteWritable((byte) 0),
          new DoubleWritable(sum));
    } catch (IOException e) {
      Util.printTaskError(e, context.getTaskAttemptID(), dummyKey);
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    context.getCounter(Counters.RUNS).increment(1);
    
    setup(context);
    map(null, null, context);
    cleanup(context);
  }
  
}
