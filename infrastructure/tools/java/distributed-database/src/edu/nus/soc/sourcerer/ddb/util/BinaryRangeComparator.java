package edu.nus.soc.sourcerer.ddb.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.util.Serialization;

public class BinaryRangeComparator
extends WritableByteArrayComparable {
  
  protected int offset;
  protected boolean relativeToEnd;
  
  public BinaryRangeComparator() { }

  public BinaryRangeComparator(byte[] value) {
    this(value, 0, false);
  }

  public BinaryRangeComparator(byte[] value, int offset, boolean relativeToEnd) {
    super(value);

    this.offset = offset;
    this.relativeToEnd = relativeToEnd;
  }

  public int getOffset() {
    return offset;
  }

  public boolean isRelativeToEnd() {
    return relativeToEnd;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    offset = in.readInt();
    relativeToEnd = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(offset);
    out.writeBoolean(relativeToEnd);
  }

  @Override
  public int compareTo(byte[] value) {
    int offset2 = 0;
    if (relativeToEnd) {
      offset2 = value.length - this.offset;
    }
    else {
      offset2 = this.offset;
    }
    return Bytes.compareTo(this.getValue(), 0, this.getValue().length,
        value, offset2, this.getValue().length);
  }
  
  //@Override
  public int compareTo(byte[] value, int offset, int length) {
    return compareTo(value);
  }

//  public static void main(String args[]) {
//    byte[] row = Bytes.toBytes("Calin-Andrei Burloiu");
//    byte[] needle = Bytes.toBytes("Andrei");
//    BinaryRangeComparator brc = new BinaryRangeComparator(needle, 14, true);
//    System.out.println(brc.compareTo(row));
//  }
}
