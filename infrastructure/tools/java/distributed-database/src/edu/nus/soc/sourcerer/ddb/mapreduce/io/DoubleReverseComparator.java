package edu.nus.soc.sourcerer.ddb.mapreduce.io;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DoubleReverseComparator
extends WritableComparator {
  
  protected DoubleReverseComparator() {
    super(DoubleWritable.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override  
  public int compare(WritableComparable a, WritableComparable b) {
    DoubleWritable d1 = (DoubleWritable)a;
    DoubleWritable d2 = (DoubleWritable)b;
    
    return -d1.compareTo(d2);
  }
  
  
}
