package edu.nus.soc.sourcerer.ddb.mapreduce.map;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;

import edu.nus.soc.sourcerer.ddb.mapreduce.Util;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

public class DEIPMapper
extends TableMapper<ByteWritable, DoubleWritable> {
  
  public enum Counters { ROWS, ERRORS, NO_RANK };

//  @Override
//  protected void setup(Context context)
//  throws IOException, InterruptedException {
//
//  }
  
  @Override
  protected void map(ImmutableBytesWritable row, Result column, Context context)
  throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    byte[] bRank = column.getValue(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
    if (bRank == null) {
      context.getCounter(Counters.NO_RANK).increment(1);
      return;
    }
    
    try {
      context.write(new ByteWritable((byte) 0x00),
          new DoubleWritable(Bytes.toDouble(bRank)));
    } catch (IOException e) {
      Util.printTaskError(e, context.getTaskAttemptID(), row);
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }

}
