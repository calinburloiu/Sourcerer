package edu.nus.soc.sourcerer.ddb.mapreduce.reduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.mapreduce.ConfigurationParams;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.EntityWritable;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHBTable;
import edu.nus.soc.sourcerer.ddb.tables.FilesHBTable;
import edu.nus.soc.sourcerer.util.ExpandableByteBuffer;

public class EntitiesReducer
extends Reducer<EntityWritable, BytesWritable, Writable, Writable> {

  protected HTable entitiesTable;
  protected HTable filesTable;
  
  public enum Counters { ROWS }

  @Override
  protected void setup(Context context)
  throws IOException, InterruptedException {
    DatabaseConfiguration.getInstance().setTablePrefix(
        context.getConfiguration().get(ConfigurationParams.TABLEPREFIX, ""));
    
    entitiesTable = EntitiesHBTable.getInstance().getHTable();
    entitiesTable.setAutoFlush(false);
    filesTable = FilesHBTable.getInstance().getHTable();
    filesTable.setAutoFlush(false);
  };
  
  @Override
  protected void reduce(EntityWritable row, Iterable<BytesWritable> values,
      Context context)
  throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    // Rows
    Put ep = new Put(Bytes.add(Bytes.toBytes(row.getFqn() + "\0"),
        row.getProjectID(), row.getFileID()));
    Put fp = new Put(Bytes.add(row.getProjectID(),
        new byte[] {row.getFileType()}, row.getFileID()));
    
    // Column qualifiers
    byte[] entitiesQual = new byte[] {row.getType()};
    byte[] filesQual = Bytes.add(new byte[] {row.getType()},
        Bytes.toBytes(row.getFqn()));
    
    // Value
    ExpandableByteBuffer ebb = new ExpandableByteBuffer(4096);
    for (BytesWritable rawValue : values) {
      ebb.put(rawValue.getBytes(), 0, rawValue.getLength());
    }
    byte[] value = ebb.getFitBytes();
    
    // Add columns.
    ep.add(EntitiesHBTable.CF_DEFAULT, entitiesQual, value);
    fp.add(FilesHBTable.CF_ENTITIES, filesQual, value);
    
    // Put to tables.
    entitiesTable.put(ep);
    filesTable.put(fp);
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    entitiesTable.flushCommits();
    filesTable.flushCommits();
  }
}
