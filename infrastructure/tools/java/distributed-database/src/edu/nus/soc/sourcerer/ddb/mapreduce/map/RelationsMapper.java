package edu.nus.soc.sourcerer.ddb.mapreduce.map;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

import edu.nus.soc.sourcerer.ddb.mapreduce.Util;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationWritable;
import edu.nus.soc.sourcerer.ddb.tables.RelationsHashHBTable;
import edu.uci.ics.sourcerer.model.File;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;

public class RelationsMapper
extends TableMapper<RelationWritable, BytesWritable> {

  public enum Counters { ROWS, ERRORS };
  
  @Override
  protected void map(ImmutableBytesWritable row, Result column, Context context)
  throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    // Output key
    byte[] kind = column.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_KIND);
    byte[] sourceID = column.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_SOURCE_ID);
    byte[] targetID = column.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_TARGET_ID);
    byte[] projectID = column.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_PROJECT_ID);
    byte[] fileID = column.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_FILE_ID);
    byte[] fileType = column.getValue(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_FILE_TYPE);
    RelationWritable key = new RelationWritable(
        kind != null ? kind[0] : (byte)
            (Relation.UNKNOWN.getValue() | RelationClass.UNKNOWN.getValue()),
        sourceID, targetID, projectID, fileID,
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
  }

}
