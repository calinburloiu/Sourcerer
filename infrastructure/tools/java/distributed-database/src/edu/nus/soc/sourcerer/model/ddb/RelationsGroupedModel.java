package edu.nus.soc.sourcerer.model.ddb;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import edu.nus.soc.sourcerer.util.EnumUtil;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.File;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;

public class RelationsGroupedModel
implements Model {
  
  protected Byte kind;
  protected byte[] sourceID;
  protected byte[] targetID;
  protected byte[] projectID;
  protected byte[] fileID;
  protected Byte fileType;
  protected List<RelationExtraModel> extra;
  
  public RelationsGroupedModel(Byte kind, byte[] sourceID, Byte sourceType, 
      byte[] targetID, Byte targetType, byte[] projectID, byte[] fileID,
      Byte fileType, int extraListCapacity) {
    super();
    this.kind = kind;
    this.sourceID = sourceID;
    this.targetID = targetID;
    this.projectID = projectID;
    this.fileID = fileID;
    this.fileType = fileType;
    extra = new ArrayList<RelationExtraModel>(extraListCapacity);
  }
  
  public RelationsGroupedModel(Byte kind, byte[] sourceID, 
      byte[] targetID, byte[] projectID, byte[] fileID,
      Byte fileType) {
    super();
    this.kind = kind;
    this.sourceID = sourceID;
    this.targetID = targetID;
    this.projectID = projectID;
    this.fileID = fileID;
    this.fileType = fileType;
    extra = new ArrayList<RelationExtraModel>();
  }
  
  public RelationsGroupedModel(Byte kind, byte[] sourceID, 
      byte[] targetID, byte[] projectID, byte[] fileID,
      Byte fileType, List<RelationExtraModel> extra) {
    super();
    this.kind = kind;
    this.sourceID = sourceID;
    this.targetID = targetID;
    this.projectID = projectID;
    this.fileID = fileID;
    this.fileType = fileType;
    this.extra = extra;
  }

  public Byte getKind() {
    return kind;
  }

  public void setKind(Byte kind) {
    this.kind = kind;
  }

  public byte[] getSourceID() {
    return sourceID;
  }

  public void setSourceID(byte[] sourceID) {
    this.sourceID = sourceID;
  }

  public byte[] getTargetID() {
    return targetID;
  }

  public void setTargetID(byte[] targetID) {
    this.targetID = targetID;
  }

  public byte[] getProjectID() {
    return projectID;
  }

  public void setProjectID(byte[] projectID) {
    this.projectID = projectID;
  }

  public byte[] getFileID() {
    return fileID;
  }

  public void setFileID(byte[] fileID) {
    this.fileID = fileID;
  }

  public Byte getFileType() {
    return fileType;
  }

  public void setFileType(Byte fileType) {
    this.fileType = fileType;
  }
  
  public List<RelationExtraModel> getExtra() {
    return extra;
  }
  
  public void setExtra(List<RelationExtraModel> extra) {
    this.extra = extra;
  }

  public void addToExtra(int offset, int length) {
    extra.add(new RelationExtraModel(offset, length));
  }
  
  public void addToExtra(RelationExtraModel extraEntity) {
    extra.add(extraEntity);
  }
  
  public RelationModel getRelationModel(Integer offset, Integer length) {
    return new RelationModel(kind, sourceID, targetID, projectID, fileID,
        offset, length);
  }
  
  public byte[] getRelationID(Integer offset, Integer length) {
    return getRelationModel(offset, length).getId();
  }

  @Override
  public String toString() {
    String str = 
        "\n  " + StringUtils.rightPad("sourceEntityID: ", 20) + Serialization.byteArrayToHexString(sourceID)
        + "\n  " + StringUtils.rightPad("relationType: ", 20) + EnumUtil.getEnumByValue(Relation.values(), RelationModel.kindToType(kind))
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {RelationModel.kindToType(kind)}) + ")"
        + "\n  " + StringUtils.rightPad("relationClass: ", 20) + EnumUtil.getEnumByValue(RelationClass.values(), RelationModel.kindToClass(kind))
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {RelationModel.kindToClass(kind)}) + ")"
        + "\n  " + StringUtils.rightPad("targetEntityID: ", 20) + Serialization.byteArrayToHexString(targetID)
        + (projectID != null ? "\n  " + StringUtils.rightPad("projectID: ", 20) + Serialization.byteArrayToHexString(projectID) : "")
        + (fileID  != null ? "\n  " + StringUtils.rightPad("fileID: ", 20) + Serialization.byteArrayToHexString(fileID) : "")
        + (fileType != null ? "\n  " + StringUtils.rightPad("fileType: ", 20) + EnumUtil.getEnumByValue(File.values(), fileType)
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {fileType}) + ")" : "")
        + (!extra.isEmpty() ? "\n  " + StringUtils.rightPad("relationIDs: ", 20)
            : "\n  " + StringUtils.rightPad("relationID: ", 20) + Serialization.byteArrayToHexString(getRelationID(null,null)))
        ;
    
    if (!extra.isEmpty()) {
      for (RelationExtraModel extraEntity : extra) {
        str += "\n    relationID: " + Serialization.byteArrayToHexString(getRelationID(extraEntity.getOffset(), extraEntity.getLength()));
        str += extraEntity;
      }
    }
    
    return str;
  }
}
