package edu.nus.soc.sourcerer.ddb.mapreduce.reduce;

import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.nus.soc.sourcerer.ddb.mapreduce.Util;

public class DEIPReducer
extends Reducer<ByteWritable, DoubleWritable, ByteWritable, DoubleWritable> {
  
  public enum Counters { ROWS, ERRORS };

//  @Override
//  protected void setup(Context context)
//      throws IOException, InterruptedException {
//
//  }
  
  @Override
  protected void reduce(ByteWritable row, Iterable<DoubleWritable> columns,
      Context context)
  throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    double sum = 0.0d;
    
    for (DoubleWritable column : columns) {
      sum += column.get();
    }
    
    try {
      context.write(new ByteWritable((byte) 0x00), new DoubleWritable(sum));
    } catch (IOException e) {
      Util.printTaskError(e, context.getTaskAttemptID(), row);
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }

}
