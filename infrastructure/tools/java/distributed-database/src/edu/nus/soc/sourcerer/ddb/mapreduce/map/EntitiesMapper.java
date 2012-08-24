package edu.nus.soc.sourcerer.ddb.mapreduce.map;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

import edu.nus.soc.sourcerer.ddb.mapreduce.Util;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.EntityWritable;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;
import edu.nus.soc.sourcerer.ddb.tables.RelationsHashHBTable;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.File;

public class EntitiesMapper
extends TableMapper<EntityWritable, BytesWritable> {

  public enum Counters { ROWS, ERRORS }

  @Override
  protected void map(ImmutableBytesWritable row, Result column, Context context)
  throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    byte[] type = column.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_ENTITYTYPE);
    byte[] fqn = column.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_FQN);
    byte[] projectID = column.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_PID);
    byte[] fileID = column.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_FID);
    byte[] fileType = column.getValue(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_FILETYPE);
    EntityWritable key = new EntityWritable(
        type != null ? type[0] : Entity.UNKNOWN.getValue(),
        fqn != null ? Bytes.toString(fqn) : "",
        projectID, fileID,
        fileType != null ? fileType[0] : File.UNKNOWN.getValue());
    
    // Output value
    byte[] offset = column.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_OFFSET);
    byte[] length = column.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_LENGTH);
    BytesWritable value;
    if (offset != null && length != null) {
      value = new BytesWritable(Bytes.add(offset, length));
    }
    else {
      value = new BytesWritable();
    }
    
    // Write key-value.
    try {
      context.write(key, value);
    } catch (IOException e) {
      Util.printTaskError(e, context.getTaskAttemptID(), row);
      context.getCounter(Counters.ERRORS).increment(1);
    }
  };
  
}
