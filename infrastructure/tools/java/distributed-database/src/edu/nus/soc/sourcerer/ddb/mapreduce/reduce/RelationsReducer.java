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
import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationWritable;
import edu.nus.soc.sourcerer.ddb.tables.FilesHBTable;
import edu.nus.soc.sourcerer.ddb.tables.RelationsDirectHBTable;
import edu.nus.soc.sourcerer.ddb.tables.RelationsInverseHBTable;
import edu.nus.soc.sourcerer.util.ExpandableByteBuffer;

public class RelationsReducer
extends Reducer<RelationWritable, BytesWritable, Writable, Writable> {

  protected HTable relationsDirectTable;
  protected HTable relationsInverseTable;
  protected HTable filesTable;
  
  public enum Counters { ROWS };
  
  @Override
  protected void setup(Context context)
  throws IOException, InterruptedException {
    DatabaseConfiguration.getInstance().setTablePrefix(
        context.getConfiguration().get(ConfigurationParams.TABLEPREFIX, ""));
    
    // Prepare tables.
    relationsDirectTable = RelationsDirectHBTable.getInstance().getHTable();
    relationsDirectTable.setAutoFlush(false);
    relationsInverseTable = RelationsInverseHBTable.getInstance().getHTable();
    relationsInverseTable.setAutoFlush(false);
    filesTable = FilesHBTable.getInstance().getHTable();
    filesTable.setAutoFlush(false);
  }

  @Override
  protected void reduce(RelationWritable row, Iterable<BytesWritable> values,
      Context context)
  throws IOException, InterruptedException {
    context.getCounter(Counters.ROWS).increment(1);
    
    // Rows
    Put rdp = new Put(Bytes.add(row.getSourceID(), new byte[] {row.getKind()},
        row.getTargetID()));
    Put rip = new Put(Bytes.add(row.getTargetID(), new byte[] {row.getKind()},
        row.getSourceID()));
    Put fp = new Put(Bytes.add(row.getProjectID(),
        new byte[] {row.getFileType()}, row.getFileID()));
    
    // Columns qualifiers
    byte[] relationsQual = Bytes.add(row.getProjectID(), row.getFileID());
    byte[] filesQual = Bytes.add(new byte[] {row.getKind()}, row.getTargetID(),
        row.getSourceID());

    // Value
    ExpandableByteBuffer ebb = new ExpandableByteBuffer(4096);
    for (BytesWritable rawValue : values) {
      ebb.put(rawValue.getBytes(), 0, rawValue.getLength());
    }
    byte[] value = ebb.getFitBytes();
    
    // Add columns.
    rdp.add(RelationsDirectHBTable.CF_DEFAULT, relationsQual, value);
    rip.add(RelationsInverseHBTable.CF_DEFAULT, relationsQual, value);
    fp.add(FilesHBTable.CF_RELATIONS, filesQual, value);
    
    // Put to tables.
    relationsDirectTable.put(rdp);
    relationsInverseTable.put(rip);
    filesTable.put(fp);
  }

  @Override
  protected void cleanup(Context context)
  throws IOException, InterruptedException {
    relationsDirectTable.flushCommits();
    relationsInverseTable.flushCommits();
    filesTable.flushCommits();
  }

}
