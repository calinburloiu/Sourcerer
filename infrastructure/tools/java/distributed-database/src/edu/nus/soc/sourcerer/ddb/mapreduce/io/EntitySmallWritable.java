package edu.nus.soc.sourcerer.ddb.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import edu.nus.soc.sourcerer.util.EnumUtil;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.Entity;

public class EntitySmallWritable
implements WritableComparable<EntitySmallWritable> {
  
  String fqn;
  Byte type;
  byte[] entityID;
  
  public EntitySmallWritable() {
    super();
    fqn = "N/A";
    type = Entity.UNKNOWN.getValue();
    entityID = new byte[16];
  }

  public EntitySmallWritable(String fqn, Byte type, byte[] entityID) {
    this();
    
    if (fqn != null)
      this.fqn = fqn;
    if (type != null)
      this.type = type;
    if (entityID != null)
      this.entityID = entityID;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(fqn);
    out.write(new byte[] { type });
    out.write(entityID);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fqn = in.readUTF();
    type = in.readByte();
    in.readFully(entityID, 0, 16);
  }

  @Override
  public int compareTo(EntitySmallWritable o) {
    return (type < o.type ? -1 : (type != o.type ? 1 :
        (fqn.compareTo(o.fqn))
      ));
  }

  @Override
  public String toString() {
    return StringUtils.rightPad(Serialization.byteArrayToHexString(entityID), 34) 
        + StringUtils.rightPad(""+EnumUtil.getEnumByValue(Entity.values(), type), 20) + fqn;
  }
  
}
