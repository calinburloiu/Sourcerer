package edu.nus.soc.sourcerer.ddb.mapreduce.reduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.nus.soc.sourcerer.ddb.mapreduce.Util;

public class CRCombiner
extends Reducer<BytesWritable, DoubleWritable, BytesWritable, DoubleWritable>{

  public enum Counters { ROWS, ERRORS, DANGLING };
  
  @Override
  protected void reduce(BytesWritable key, Iterable<DoubleWritable> values,
      Context context)
  throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    double surfing = 0.0d;
    boolean danglingEntity = false;
    
    for (DoubleWritable value : values) {
      if (value.get() == Double.MAX_VALUE) {
        danglingEntity = true;
        continue;
      }
      
      // Sum the contribution of each inbound relation.
      surfing += value.get();
    }
    
    try {
      context.write(key, new DoubleWritable(surfing));
      
      if (danglingEntity) {
        context.write(key, new DoubleWritable(Double.MAX_VALUE));
        context.getCounter(Counters.DANGLING).increment(1);
      }
    } catch (IOException e) {
      Util.printTaskError(e, context.getTaskAttemptID(), key);
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }

}
