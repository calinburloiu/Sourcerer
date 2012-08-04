package edu.nus.soc.sourcerer.ddb.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.sourcerer.model.Entity;

public class RelationSourceWritable
    implements WritableComparable<RelationSourceWritable> {
  
  byte sourceType;
  byte[] sourceID;
  
  public RelationSourceWritable() {
    super();
    sourceType = Entity.UNKNOWN.getValue();
    sourceID = new byte[16];
  }

  public RelationSourceWritable(byte sourceType, byte[] sourceID) {
    super();
    this.sourceType = sourceType;
    this.sourceID = sourceID;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(sourceType);
    out.write(sourceID);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    sourceType = in.readByte();
    in.readFully(sourceID, 0, 16);
  }

  @Override
  public int compareTo(RelationSourceWritable arg0) {
    return Bytes.compareTo(sourceID, arg0.getSourceID());
  }

  public byte getSourceType() {
    return sourceType;
  }

  public byte[] getSourceID() {
    return sourceID;
  }

  
}
