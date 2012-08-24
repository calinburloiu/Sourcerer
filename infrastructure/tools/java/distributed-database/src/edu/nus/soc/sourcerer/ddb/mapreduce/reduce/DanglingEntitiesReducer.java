package edu.nus.soc.sourcerer.ddb.mapreduce.reduce;

import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

@Deprecated
public class DanglingEntitiesReducer
extends Reducer<ByteWritable, DoubleWritable, ByteWritable, DoubleWritable> {
  
  enum Counters { REDUCES, ERRORS }

  @Override
  protected void reduce(ByteWritable keyIn, Iterable<DoubleWritable> valuesIn,
      Context context) throws IOException, InterruptedException {
    
    context.getCounter(Counters.REDUCES).increment(1);
    
    double sum = 0.0d;
    
    for (DoubleWritable valueIn : valuesIn) {
      sum += valueIn.get();
    }
    
    try {
      context.write(new ByteWritable((byte) 0),
          new DoubleWritable(sum));
    } catch (IOException e) {
      context.getCounter(Counters.ERRORS).increment(1);
      System.err.println("I/O error: " + StringUtils.stringifyException(e));
    }
  };
  
  
}
