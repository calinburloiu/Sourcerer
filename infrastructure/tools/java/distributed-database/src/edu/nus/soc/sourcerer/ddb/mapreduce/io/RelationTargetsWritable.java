package edu.nus.soc.sourcerer.ddb.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;

public class RelationTargetsWritable implements Writable {

  byte kind;
  byte targetType;
  byte[] targetID;
  byte[] relationID;
  
  public RelationTargetsWritable() {
    super();
    kind = (byte)
        (Relation.UNKNOWN.getValue() | RelationClass.UNKNOWN.getValue());
    targetType = Entity.UNKNOWN.getValue();
    targetID = new byte[16];
    relationID = new byte[16];
  }

  public RelationTargetsWritable(byte kind, byte targetType,
      byte[] targetID, byte[] relationID) {
    super();
    this.kind = kind;
    this.targetType = targetType;
    this.targetID = targetID;
    this.relationID = relationID;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(kind);
    out.writeByte(targetType);
    out.write(targetID);
    out.write(relationID);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    kind = in.readByte();
    targetType = in.readByte();
    in.readFully(targetID, 0, 16);
    in.readFully(relationID, 0, 16);
  }

  public byte getKind() {
    return kind;
  }

  public byte getTargetType() {
    return targetType;
  }

  public byte[] getTargetID() {
    return targetID;
  }

  public byte[] getRelationID() {
    return relationID;
  }
  
  public static class RelationTargetsSerialization {
    
    protected int count = 0;
    protected ByteBuffer targetsBuf;
    protected ByteBuffer relationsBuf;
    
    public RelationTargetsSerialization(int initCapacity) {
      targetsBuf = ByteBuffer.allocate(initCapacity);
      relationsBuf = ByteBuffer.allocate(initCapacity);
    }
    
    public void write(Iterable<RelationTargetsWritable> targets) {
      for (RelationTargetsWritable target : targets) {
        count++;
        
        do {
          try {
            writeTargetEntityBytes(target);
            break;
          } catch (BufferOverflowException e) {
            targetsBuf = Serialization.reallocateByteBuffer(targetsBuf);
          }
        } while (true);
        
        do {
          try {
            writeRelationIDBytes(target);
            break;
          } catch (BufferOverflowException e) {
            relationsBuf = Serialization.reallocateByteBuffer(relationsBuf);
          }
        } while (true);
      }
    }
    
    protected void writeTargetEntityBytes(RelationTargetsWritable target) {
      targetsBuf.put(Bytes.add(new byte[] {target.getKind()},
          new byte[] {target.getTargetType()}, target.getTargetID()));
    }
    
    protected void writeRelationIDBytes(RelationTargetsWritable target) {
      relationsBuf.put(target.getRelationID());
    }

    public int getCount() {
      return count;
    }

    public ByteBuffer getTargetsByteBuffer() {
      return targetsBuf;
    }

    public ByteBuffer getRelationsByteBuffer() {
      return relationsBuf;
    }

    public byte[] getTargetsBytes() {
      return Serialization.getFitByteBufferBytes(targetsBuf);
    }

    public byte[] getRelationsBytes() {
      return Serialization.getFitByteBufferBytes(relationsBuf);
    }
  }
  
//  public static void main(String args[]) {
//    ArrayList<RelationTargetsWritable> list = new ArrayList<RelationTargetsWritable>();
//    RelationTargetsWritable elem = new RelationTargetsWritable((byte) 0xAA, (byte) 0xBB,
//        new byte[] {(byte)0x00, (byte)0x01, (byte)0x02, (byte)0x03, (byte)0x04, (byte)0x05, (byte)0x06, (byte)0x07, (byte)0x08, (byte)0x09, (byte)0x0A, (byte)0x0B, (byte)0x0C, (byte)0x0D, (byte)0x0E, (byte)0x0F},
//        new byte[] {(byte)0x00, (byte)0x10, (byte)0x20, (byte)0x30, (byte)0x40, (byte)0x50, (byte)0x60, (byte)0x70, (byte)0x80, (byte)0x90, (byte)0xA0, (byte)0xB0, (byte)0xC0, (byte)0xD0, (byte)0xE0, (byte)0xF0});
// 
//    int capac = 3;
//    for (int i=0; i<capac; i++) {
//      list.add(elem);
//    }
//    
//    RelationTargetsSerialization rts = new RelationTargetsSerialization(1);
//    rts.write(list);
//    System.out.println(rts.getCount());
////    System.out.println(Serialization.byteArrayToHexString(rts.getTargetsByteBuffer().array()));
////    System.out.println(Serialization.byteArrayToHexString(rts.getRelationsByteBuffer().array()));
//    System.out.println(Serialization.byteArrayToHexString(rts.getTargetsBytes()));
//    System.out.println(Serialization.byteArrayToHexString(rts.getRelationsBytes()));
//  }
}
