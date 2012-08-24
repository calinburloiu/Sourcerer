package edu.nus.soc.sourcerer.ddb.mapreduce.reduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;

import edu.nus.soc.sourcerer.ddb.mapreduce.ConfigurationParams;
import edu.nus.soc.sourcerer.ddb.mapreduce.Util;
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.CRMetricsJob;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

public class CRReducer
extends TableReducer<BytesWritable, DoubleWritable, ImmutableBytesWritable> {

  protected long entitiesCount;
  /** Dangling Entities Inner Product */
  protected double deip;
  protected float teleportationProbab;
  
  public enum Counters { ROWS, ERRORS, WRITTEN, DANGLING };
  
  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    
    // Read entities count.
    entitiesCount = conf.getLong(ConfigurationParams.ENTITIES_COUNT,
        Long.MAX_VALUE);
    
    // Read DEIP from metrics file if one of the metrics should be calculated
    // or from DEIP file otherwise.
    boolean crMetricsDEIP =
        conf.getBoolean(ConfigurationParams.CRMETRICS_DEIP, false);
    if (!crMetricsDEIP) {
      deip = Util.readDEIPFromDEIPFile(
          conf.get(ConfigurationParams.DEIP_FILE), conf);
    }
    else {
      deip = Util.readMetricFromFile(
          conf.get(ConfigurationParams.CRMETRICS_FILE),
          CRMetricsJob.Calcs.DEIP, conf);
    }
    
    // Read teleportation probability value.
    teleportationProbab = conf.getFloat(ConfigurationParams.CR_TELEPORTATION,
        0.15f);
  }
  
  @Override
  protected void reduce(BytesWritable key, Iterable<DoubleWritable> values,
      Context context)
  throws IOException, InterruptedException {
    
    context.getCounter(Counters.ROWS).increment(1);
    
    byte[] sourceID = Bytes.head(key.getBytes(), key.getLength());
    
    // Code entities surfing component of CodeRank.
    double surfing = 0.0d;
    // Dangling code entities component of CodeRank.
    double dangling = deip / entitiesCount;
    // CodeRank component for teleportation from a code entity to another.
    double teleportation = 1.0d / entitiesCount;
    // Variable that marks is the current entity is dangling.
    boolean danglingEntity = false;
    
    for (DoubleWritable value : values) {
      if (value.get() == Double.MAX_VALUE) {
        danglingEntity = true;
        continue;
      }
      
      // Sum the contribution of each inbound relation.
      surfing += value.get();
    }
    
    // Compute CodeRank.
    double codeRank = (1.0f - teleportationProbab) * (surfing + dangling)
        + teleportationProbab * teleportation;
    
    // Output CodeRank for the current entity.
    try {
      Put put = new Put(sourceID);
      put.add(EntitiesHashHBTable.CF_RELATIONS,
          EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK,
          Bytes.toBytes(codeRank));
      context.write(new ImmutableBytesWritable(sourceID), put);
      context.getCounter(Counters.WRITTEN).increment(1);
      
      // If this is a dangling entity, cache it to the end of the table for fast
      // retrieval during DEIP calculation.
      if (danglingEntity) {
        byte[] danglingKey = Bytes.add(
            EntitiesHashHBTable.DANGLING_CACHE_START_ROW, sourceID);
        Put putDangling = new Put(danglingKey);
        putDangling.add(EntitiesHashHBTable.CF_RELATIONS,
            EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK,
            Bytes.toBytes(codeRank));
        context.write(new ImmutableBytesWritable(danglingKey), putDangling);
        context.getCounter(Counters.DANGLING).increment(1);
      }
    } catch (IOException e) {
      Util.printTaskError(e, context.getTaskAttemptID(), key);
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }

}
