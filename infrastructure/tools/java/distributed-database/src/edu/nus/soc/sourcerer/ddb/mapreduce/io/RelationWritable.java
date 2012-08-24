package edu.nus.soc.sourcerer.ddb.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.File;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;

public class RelationWritable
implements WritableComparable<RelationWritable> {

  protected byte kind;
  protected byte[] sourceID;
  protected byte[] targetID;
  protected byte[] projectID;
  protected byte[] fileID;
  protected byte fileType;
  
  public RelationWritable() {
    super();
    
    kind = (byte)
        (Relation.UNKNOWN.getValue() | RelationClass.UNKNOWN.getValue());
    sourceID = new byte[16];
    targetID = new byte[16];
    projectID = new byte[16];
    fileID = new byte[16];
    fileType = File.UNKNOWN.getValue();
  }

  public RelationWritable(byte kind, byte[] sourceID, byte[] targetID,
      byte[] projectID, byte[] fileID, byte fileType) {
    super();
    
    this.kind = kind;
    
    if (sourceID != null)
      this.sourceID = sourceID;
    else
      this.sourceID = new byte[16];
    
    if (targetID != null)
      this.targetID = targetID;
    else
      this.targetID = new byte[16];
    
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
  public int compareTo(RelationWritable o) {
    int cmpProject = Bytes.compareTo(projectID, o.getProjectID());
    if (cmpProject != 0)
      return cmpProject;
    
    int cmpFile = Bytes.compareTo(fileID, o.getFileID());
    if (cmpFile != 0)
      return cmpFile;
      
    int cmpSource = Bytes.compareTo(sourceID, o.getSourceID());
    if (cmpSource != 0)
      return cmpSource;
    
    if (kind < o.kind)
      return -1;
    else if (kind > o.kind)
      return 1;
    
    return Bytes.compareTo(targetID, o.getTargetID());
  }

  public byte getKind() {
    return kind;
  }

  public byte[] getSourceID() {
    return sourceID;
  }

  public byte[] getTargetID() {
    return targetID;
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

  @Override
  public void write(DataOutput out) throws IOException {
    if (sourceID.length != 16)
      throw new IOException("sourceID " + Serialization.byteArrayToHexString(sourceID)
          + " does not have 16 bytes");
    if (targetID.length != 16)
      throw new IOException("targetID: " + Serialization.byteArrayToHexString(targetID)
          + " does not have 16 bytes");
    if (projectID.length != 16)
      throw new IOException("projectID: " + Serialization.byteArrayToHexString(projectID)
          + " does not have 16 bytes");
    if (fileID.length != 16)
      throw new IOException("fileID: " + Serialization.byteArrayToHexString(fileID)
          + " does not have 16 bytes");
    
    out.writeByte(kind);
    out.write(sourceID);
    out.write(targetID);
    out.write(projectID);
    out.write(fileID);
    out.writeByte(fileType);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    kind = in.readByte();
    in.readFully(sourceID, 0, 16);
    in.readFully(targetID, 0, 16);
    in.readFully(projectID, 0, 16);
    in.readFully(fileID, 0, 16);
    fileType = in.readByte();
  }
  
}
