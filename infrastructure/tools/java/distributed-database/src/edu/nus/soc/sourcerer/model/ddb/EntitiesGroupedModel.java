package edu.nus.soc.sourcerer.model.ddb;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import edu.nus.soc.sourcerer.util.EnumUtil;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.Entity;

public class EntitiesGroupedModel
implements Model {

  protected String fqn = null;
  protected byte[] projectID = null;
  protected byte[] fileID = null;
  protected Byte fileType = null;
  protected Byte type = null;
  protected List<EntityExtraModel> extra = null;
  
  public EntitiesGroupedModel() {
  }
  
  public EntitiesGroupedModel(String fqn, byte[] projectID, byte[] fileID,
      Byte type) {
    this(fqn, projectID, fileID, type,
        new ArrayList<EntityExtraModel>());
  }
  
  public EntitiesGroupedModel(String fqn, byte[] projectID, byte[] fileID,
      Byte type, int extraListCapac) {
    this(fqn, projectID, fileID, type,
        new ArrayList<EntityExtraModel>(extraListCapac));
  }

  public EntitiesGroupedModel(String fqn, byte[] projectID, byte[] fileID,
      Byte type, List<EntityExtraModel> extra) {
    super();
    this.fqn = fqn;
    this.projectID = projectID;
    this.fileID = fileID;
    this.type = type;
    this.extra = extra;
  }

  public Byte getType() {
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

  public void setType(Byte type) {
    this.type = type;
  }

  public void setFqn(String fqn) {
    this.fqn = fqn;
  }

  public void setProjectID(byte[] projectID) {
    this.projectID = projectID;
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

  public List<EntityExtraModel> getExtra() {
    return extra;
  }

  public void setExtra(List<EntityExtraModel> extra) {
    this.extra = extra;
  }
  
  public void addToExtra(EntityExtraModel extraEntity) {
    this.extra.add(extraEntity);
  }
  
  public void addToExtra(int modifiers, int multi, int offset, int length) {
    this.extra.add(new EntityExtraModel(offset, length, modifiers, multi));
  }
  
  public EntityModel getEntityModel(Integer modifiers, Integer multi,
      Integer offset, Integer length) {
    return new EntityModel(type, fqn, projectID, fileID, modifiers, multi,
        offset, length, null, null, null);
  }
  
  public byte[] getEntityID(Integer modifiers, Integer multi, Integer offset,
      Integer length) {
    return getEntityModel(modifiers, multi, offset, length).getId();
  }

  @Override
  public String toString() {
    String str = "\n  " + StringUtils.rightPad("fqn: ", 16) + fqn
        + "\n  " + StringUtils.rightPad("type: ", 16) + EnumUtil.getEnumByValue(Entity.values(), type)
          + "(0x" + Serialization.byteArrayToHexString(new byte[] {type}) + ")"
        + (projectID != null ? "\n  " + StringUtils.rightPad("projectID: ", 16) + Serialization.byteArrayToHexString(projectID) : "")
        + (fileID != null ? "\n  " + StringUtils.rightPad("fileID: ", 16) + Serialization.byteArrayToHexString(fileID) : "")
        + (!extra.isEmpty() ? "\n  " + StringUtils.rightPad("entityIDs: ", 16)
            : "\n  " + StringUtils.rightPad("entityID: ", 16) 
                + Serialization.byteArrayToHexString(getEntityID(null,null,null,null)))
        ;
    
    if (!extra.isEmpty()) {
      for (EntityExtraModel extraEntity : extra) {
        str += "\n    entityID: " + Serialization.byteArrayToHexString(
            getEntityID(extraEntity.getModifiers(), extraEntity.getMulti(),
                extraEntity.getOffset(), extraEntity.getLength()));
        str += extraEntity;
      }
    }
    
    return str;
  }

}
