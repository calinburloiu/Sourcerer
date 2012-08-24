package edu.nus.soc.sourcerer.ddb.mapreduce.map;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.StringUtils;

import edu.nus.soc.sourcerer.ddb.queries.EntitiesRetriever;
import edu.nus.soc.sourcerer.model.ddb.SourcedRelationsModel;
import edu.nus.soc.sourcerer.model.ddb.SourcedRelationsModel.RelationTargetModel;

public class CRMapper extends TableMapper<BytesWritable, DoubleWritable> {

  protected long entitiesCount;
  
  public enum Counters { ROWS, ERRORS,
    CONTRIBUTIONS, DANGLING }; 
  
  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
//    entitiesCount = context.getConfiguration().getLong(
//        ConfigurationParams.ENTITIES_COUNT, 0l);
  }

  @Override
  protected void map(ImmutableBytesWritable row, Result columns, Context context)
  throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    SourcedRelationsModel sourcedRelations = 
        EntitiesRetriever.entitiesHashResultToSourcedRelationsModel(columns);
    Double valueOut = null;
    if (sourcedRelations.getSourceRank() != null
        && sourcedRelations.getRelationsCount() != 0) {
      valueOut = sourcedRelations.getSourceRank()
          / sourcedRelations.getRelationsCount();
    }
    
    try {
      // Output a zero contribution of source entities such that entities that
      // are not targets of any other entity will be submit with 0 rank.
      if (sourcedRelations.getRelationsCount() != 0) {
        context.write(new BytesWritable(columns.getRow()),
            new DoubleWritable(0.0d));
      }
      // Dangling entities are marked differently in order to be cached in
      // entities_hash table during reduce phase for fast calculation of DEIP.
      else {
        context.write(new BytesWritable(columns.getRow()),
            new DoubleWritable(Double.MAX_VALUE));
        context.getCounter(Counters.DANGLING).increment(1);
      }
      
      // Output the contribution of the source entity to each target entity.
      if (valueOut != null) {
        for (RelationTargetModel target : sourcedRelations.getRelations()) {
          context.write(new BytesWritable(target.getTargetID()),
              new DoubleWritable(valueOut));
          context.getCounter(Counters.CONTRIBUTIONS).increment(1);
        }
      }
    } catch (IOException e) {
      System.err.println("Output error for row "
          + Bytes.toStringBinary(row.get()) + ": "
          + StringUtils.stringifyException(e));
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }
}
