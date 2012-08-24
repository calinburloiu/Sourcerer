package edu.nus.soc.sourcerer.ddb.mapreduce.map;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import edu.nus.soc.sourcerer.ddb.mapreduce.ConfigurationParams;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

public class CRInitMapper 
extends TableMapper<ImmutableBytesWritable, Writable> {
  
  protected long entitiesCount;
  
  public enum Counters { ROWS, ERRORS, NULL_TEC, DANGLING }

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    // Read entities count.
    entitiesCount = context.getConfiguration().getLong(
        ConfigurationParams.ENTITIES_COUNT, 0);
  }
  
  @Override
  protected void map(ImmutableBytesWritable row, Result columns,
      Context context) throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    byte[] bRelationsCount = columns.getValue(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_TARGETS_COUNT);
    
    try {
      // Write initial values for entity's variables (rank and tec).
      Put put = new Put(columns.getRow());
      put.add(EntitiesHashHBTable.CF_RELATIONS,
          EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK,
          Bytes.toBytes(1.0d / entitiesCount));
      if (bRelationsCount == null) {
        put.add(EntitiesHashHBTable.CF_RELATIONS,
            EntitiesHashHBTable.COL_RELATIONS_TARGETS_COUNT,
            Bytes.toBytes((int) 0));
        context.getCounter(Counters.NULL_TEC).increment(1);
      }      
      context.write(new ImmutableBytesWritable(columns.getRow()), put);
      
      // If this entity is dangling write its initial values for variables.
      if (bRelationsCount == null
          || (bRelationsCount != null && Bytes.toInt(bRelationsCount) == 0)) {
        byte[] danglingKey = Bytes.add(
            EntitiesHashHBTable.DANGLING_CACHE_START_ROW, columns.getRow());
        put = new Put(danglingKey);
        put.add(EntitiesHashHBTable.CF_RELATIONS,
            EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK,
            Bytes.toBytes(1.0d / entitiesCount));
        context.write(new ImmutableBytesWritable(danglingKey), put);
        context.getCounter(Counters.DANGLING).increment(1);
      }
    } catch (IOException e) {
      System.err.println("Mapper could not write row "
          + Bytes.toStringBinary(row.get()));
      context.getCounter(Counters.ERRORS).increment(1);
    }
  }
}
