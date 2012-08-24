package edu.nus.soc.sourcerer.model.ddb;

import org.apache.commons.lang.StringUtils;

import edu.nus.soc.sourcerer.util.EnumUtil;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.File;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;

public class RelationModel extends ModelWithID {
  protected Byte kind;
  protected byte[] sourceID;
  protected Byte sourceType;
  protected byte[] targetID;
  protected Byte targetType;
  protected byte[] projectID;
  protected byte[] fileID;
  protected Byte fileType;
  protected Integer offset;
  protected Integer length;
  
  public RelationModel(Byte kind, byte[] sourceID, byte[] targetID,
      byte[] projectID, byte[] fileID, Integer offset, Integer length) {
    super();
    this.kind = kind;
    this.sourceID = sourceID;
    this.targetID = targetID;
    this.projectID = projectID;
    this.fileID = fileID;
    this.offset = offset;
    this.length = length;
    
    // Compute ID.
    id = computeId(73, "kind", "sourceID", "targetID", "projectID", "fileID",
        "offset", "length");
  }
  
  public RelationModel(Byte kind, byte[] sourceID,
      byte[] targetID, byte[] projectID, byte[] fileID,
      Integer offset, Integer length, Byte fileType) {
    this(kind, sourceID, targetID, projectID, fileID, offset, length);
    this.fileType = fileType;
  }
  
  public RelationModel(Byte kind, byte[] sourceID,
      byte[] targetID, byte[] projectID, byte[] fileID,
      Integer offset, Integer length, Byte sourceType, Byte targetType,
      Byte fileType) {
    this(kind, sourceID, targetID, projectID, fileID, offset, length, fileType);
    this.sourceType = sourceType;
    this.targetType = targetType;
  }
  
  public static byte kindToType(byte kind) {
    return (byte) (kind & 0x1f);
  }
  
  public static byte kindToClass(byte kind) {
    return (byte) (kind & 0xe0);
  }

  public Byte getType() {
    return kindToType(kind);
  }

  public Byte getRelationClass() {
    return kindToClass(kind);
  }
  
  /**
   * In HBase both the type and the class of the relation are encoded in 1 byte.
   * The 3 most significant bits are used for class and the rest of them for
   * type.
   * 
   * @return
   */
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

  public Integer getOffset() {
    return offset;
  }

  public Integer getLength() {
    return length;
  }

  public Byte getSourceType() {
    return sourceType;
  }

  public Byte getTargetType() {
    return targetType;
  }

  public Byte getFileType() {
    return fileType;
  }

  public void setKind(Byte kind) {
    this.kind = kind;
  }

  public void setSourceID(byte[] sourceID) {
    this.sourceID = sourceID;
  }

  public void setTargetID(byte[] targetID) {
    this.targetID = targetID;
  }

  public void setProjectID(byte[] projectID) {
    this.projectID = projectID;
  }

  public void setFileID(byte[] fileID) {
    this.fileID = fileID;
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
  }

  public void setLength(Integer length) {
    this.length = length;
  }

  public void setSourceType(Byte sourceType) {
    this.sourceType = sourceType;
  }

  public void setTargetType(Byte targetType) {
    this.targetType = targetType;
  }

  public void setFileType(Byte fileType) {
    this.fileType = fileType;
  }

  @Override
  public String toString() {
    return Serialization.byteArrayToHexString(id)
        + "\n  " + StringUtils.rightPad("sourceEntityID: ", 18) + Serialization.byteArrayToHexString(sourceID)
        + (sourceType != null ? "\n  " + StringUtils.rightPad("sourceEntityType: ", 18) + EnumUtil.getEnumByValue(Entity.values(), sourceType)
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {sourceType}) + ")" : "")
        + "\n  " + StringUtils.rightPad("relationType: ", 18) + EnumUtil.getEnumByValue(Relation.values(), RelationModel.kindToType(kind))
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {RelationModel.kindToType(kind)}) + ")"
        + "\n  " + StringUtils.rightPad("relationClass: ", 18) + EnumUtil.getEnumByValue(RelationClass.values(), RelationModel.kindToClass(kind))
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {RelationModel.kindToClass(kind)}) + ")"
        + "\n  " + StringUtils.rightPad("targetEntityID: ", 18) + Serialization.byteArrayToHexString(targetID)
        + (targetType != null ? "\n  " + StringUtils.rightPad("targetEntityType: ", 18) + EnumUtil.getEnumByValue(Entity.values(), targetType)
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {targetType}) + ")" : "")
        + (projectID != null ? "\n  " + StringUtils.rightPad("projectID: ", 18) + Serialization.byteArrayToHexString(projectID) : "")
        + (fileID  != null ? "\n  " + StringUtils.rightPad("fileID: ", 18) + Serialization.byteArrayToHexString(fileID) : "")
        + (fileType != null ? "\n  " + StringUtils.rightPad("fileType: ", 18) + EnumUtil.getEnumByValue(File.values(), fileType)
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {fileType}) + ")" : "")
        + (offset != null ? "\n  " + StringUtils.rightPad("offset: ", 18) + offset : "")
        + (length != null ? "\n  " + StringUtils.rightPad("length: ", 18) + length : "")
        ;
  }

}
