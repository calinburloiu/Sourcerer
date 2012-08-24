package edu.nus.soc.sourcerer.ddb.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.File;

public class EntityWritable
implements WritableComparable<EntityWritable> {

  protected byte type;
  protected String fqn;
  protected byte[] projectID;
  protected byte[] fileID;
  protected byte fileType;
  
  public EntityWritable() {
    super();
    
    this.type = Entity.UNKNOWN.getValue();
    this.fqn = "";
    this.projectID = new byte[16];
    this.fileID = new byte[16];
    this.fileType = File.UNKNOWN.getValue();
  }

  public EntityWritable(byte type, String fqn, byte[] projectID, byte[] fileID,
      byte fileType) {
    super();
    
    this.type = type;

    if (fqn != null)
      this.fqn = fqn;
    else
      this.fqn = "";
    
    if (projectID != null) 
      this.projectID = projectID;
    else
      this.projectID = new byte[16];
    
    if (fileID != null)
      this.fileID = fileID;
    else
      this.fileID = new byte[16];
    
    this.fileType = fileType;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (projectID.length != 16)
      throw new IOException("projectID: " + Serialization.byteArrayToHexString(projectID)
          + " does not have 16 bytes");
    if (fileID.length != 16)
      throw new IOException("fileID: " + Serialization.byteArrayToHexString(fileID)
          + " does not have 16 bytes");
    
    out.writeByte(type);
    out.writeUTF(fqn);
    out.write(projectID);
    out.write(fileID);
    out.writeByte(fileType);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type = in.readByte();
    fqn = in.readUTF();
    in.readFully(projectID);
    in.readFully(fileID);
    fileType = in.readByte();
  }

  @Override
  public int compareTo(EntityWritable o) {
    if (type < o.type)
      return -1;
    if (type > o.type)
      return 1;
    
    int cmpFQN = fqn.compareTo(o.getFqn());
    if (cmpFQN != 0)
      return cmpFQN;
    
    int cmpProjectID = Bytes.compareTo(projectID, o.getProjectID());
    if (cmpProjectID != 0)
      return cmpProjectID;
    
    int cmpFileID = Bytes.compareTo(fileID, o.getFileID());
    if (cmpFileID != 0)
      return cmpFileID;
    
    if (fileType < o.getFileType())
      return -1;
    if (fileType > o.getFileType())
      return 1;
    
    return 0;
  }

  public byte getType() {
    return type;
  }

  public String getFqn() {
    return fqn;
  }

  public byte[] getProjectID() {
    return projectID;
  }

  public byte[] getFileID() {
    return fileID;
  }

  public byte getFileType() {
    return fileType;
  }

}
