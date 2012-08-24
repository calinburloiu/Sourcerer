package edu.nus.soc.sourcerer.ddb.mapreduce.reduce;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.nus.soc.sourcerer.ddb.mapreduce.Util;
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.CRMetricsJob.Calcs;

public class CRMetricsCombiner
extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  public enum Counters { ERRORS, DIST_CALCS };
  
  @Override
  protected void reduce(Text keyIn, Iterable<DoubleWritable> valuesIn,
      Context context)
  throws IOException, InterruptedException {
    double sum = 0.0d;
    
    for (DoubleWritable valueIn : valuesIn) {
      sum += valueIn.get();
    }
    
    if (Bytes.compareTo(Bytes.head(keyIn.getBytes(), keyIn.getLength()),
        Bytes.toBytes(Calcs.DIST.getValue())) == 0) {
      sum = finishOperation(sum);
      context.getCounter(Counters.DIST_CALCS).increment(1);
    }
    
    try {
      context.write(keyIn, new DoubleWritable(sum));
    } catch (IOException e) {
      Util.printTaskError(e, context.getTaskAttemptID(), keyIn);
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }
  
  protected double finishOperation(double d) {
    return d;
  }

}
