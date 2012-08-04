package edu.nus.soc.sourcerer.ddb.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

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
}
