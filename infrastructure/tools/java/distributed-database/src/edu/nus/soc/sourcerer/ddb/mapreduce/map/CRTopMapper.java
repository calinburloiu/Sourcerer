package edu.nus.soc.sourcerer.ddb.mapreduce.map;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;

import edu.nus.soc.sourcerer.ddb.mapreduce.Util;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.EntitySmallWritable;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;
import edu.uci.ics.sourcerer.model.Entity;

public class CRTopMapper
extends TableMapper<DoubleWritable, EntitySmallWritable> {
  
  public enum Counters { ROWS, ERRORS };

  @Override
  protected void map(ImmutableBytesWritable row, Result column, Context context)
  throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    byte[] entityID = column.getRow();
    byte[] bType = column.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_ENTITYTYPE);
    byte[] bFQN = column.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_FQN);
    byte[] bRank = column.getValue(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
    
    if (bRank != null) {
      try {
        EntitySmallWritable entity = new EntitySmallWritable(
            bFQN != null ? Bytes.toString(bFQN) : null,
            bType != null ? bType[0] : Entity.UNKNOWN.getValue(), entityID);
        context.write(new DoubleWritable(Bytes.toDouble(bRank)), entity);
      } catch (IOException e) {
        Util.printTaskError(e, context.getTaskAttemptID(), row);
        context.getCounter(Counters.ERRORS).increment(1);
      }
    }
  }

}
