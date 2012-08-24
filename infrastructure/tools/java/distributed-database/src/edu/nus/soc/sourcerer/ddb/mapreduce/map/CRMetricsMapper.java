package edu.nus.soc.sourcerer.ddb.mapreduce.map;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import edu.nus.soc.sourcerer.ddb.mapreduce.ConfigurationParams;
import edu.nus.soc.sourcerer.ddb.mapreduce.Util;
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.CRMetricsJob.Calcs;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

public class CRMetricsMapper
extends TableMapper<Text, DoubleWritable> {
  
  protected boolean calcSum;
  protected boolean calcDist;
  protected boolean calcDEIP;
  
  public enum Counters { ROWS, ERRORS,
    MISSING_LAST_RANK, MISSING_PREV_RANK, MISSING_TARGETS_COUNT };

  @Override
  protected void setup(Context context)
  throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    
    calcSum = conf.getBoolean(ConfigurationParams.CRMETRICS_SUM, false);
    calcDist = conf.getBoolean(ConfigurationParams.CRMETRICS_DIST, false);
    calcDEIP = conf.getBoolean(ConfigurationParams.CRMETRICS_DEIP, false);
  }
  
  @Override
  protected void map(ImmutableBytesWritable row, Result column, Context context)
  throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    // Get ranks.
    List<KeyValue> kvRanks = column.getColumn(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
    double rank1 = Double.MAX_VALUE, rank2 = Double.MAX_VALUE;
    try {
      rank2 = Bytes.toDouble(kvRanks.get(0).getValue());
      if (calcDist) {
        rank1 = Bytes.toDouble(kvRanks.get(1).getValue());
      }
    } catch (IndexOutOfBoundsException e) {
      if (rank2 == Double.MAX_VALUE) {
        context.getCounter(Counters.MISSING_LAST_RANK).increment(1);        
      }
      if (calcDist && rank1 == Double.MAX_VALUE) {
        context.getCounter(Counters.MISSING_PREV_RANK).increment(1);
      }
    }
    
    // Get targets count.
    int targetsCount = Integer.MAX_VALUE;
    if (calcDEIP) {
      byte[] bTargetsCount = column.getValue(EntitiesHashHBTable.CF_RELATIONS,
          EntitiesHashHBTable.COL_RELATIONS_TARGETS_COUNT);
      if (bTargetsCount != null) {
        targetsCount = Bytes.toInt(bTargetsCount);
      }
    }
    
    try {
      if (calcSum && rank2 != Double.MAX_VALUE) {
        context.write(new Text(Calcs.SUM.getValue()),
            new DoubleWritable(rank2));
      }
      if (calcDist && rank2 != Double.MAX_VALUE && rank1 != Double.MAX_VALUE) {
        context.write(new Text(Calcs.DIST.getValue()),
            new DoubleWritable(Math.pow(rank2 - rank1, 2)));
      }
      if (calcDEIP && rank2 != Double.MAX_VALUE && targetsCount == 0) {
        context.write(new Text(Calcs.DEIP.getValue()),
            new DoubleWritable(rank2));
      }
    } catch (IOException e) {
      Util.printTaskError(e, context.getTaskAttemptID(), row);
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }

}
