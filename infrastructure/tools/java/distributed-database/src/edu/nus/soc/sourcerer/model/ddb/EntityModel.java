package edu.nus.soc.sourcerer.model.ddb;

import org.apache.commons.lang.StringUtils;

import edu.nus.soc.sourcerer.util.EnumUtil;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.File;
import edu.uci.ics.sourcerer.model.Modifier;


public class EntityModel extends ModelWithID {
  protected Byte type = null;
  protected String fqn = null;
  protected byte[] projectID = null;
  protected byte[] fileID = null;
  protected Integer modifiers = null;
  protected Integer multi = null;
  protected Integer offset = null;
  protected Integer length = null;
  
  // Metrics
  protected Integer loc = null;
  protected Integer nwloc = null;
  
  // Extra
  protected Byte fileType = null;
  
  public EntityModel() {
  }

  public EntityModel(Byte type, String fqn, byte[] projectID,
      byte[] fileID, Integer modifier, Integer multi, Integer offset,
      Integer length, Integer loc, Integer nwloc, Byte fileType) {
    super();
    this.type = type;
    this.fqn = fqn;
    this.modifiers = modifier;
    this.multi = multi;
    this.projectID = projectID;
    this.fileID = fileID;
    this.offset = offset;
    this.length = length;
    this.loc = loc;
    this.nwloc = nwloc;
    this.fileType = fileType;
    
    // Compute ID.
    id = computeId(8235, "type", "fqn", "projectID", "fileID", "modifiers",
        "multi", "offset", "length");
  }
  
  public EntityModel(byte[] entityID, Byte type, String fqn,
      byte[] projectID, byte[] fileID, Integer modifiers, Integer multi,
      Integer offset, Integer length, Integer loc, Integer nwloc,
      Byte fileType) {
    super(entityID);
    this.type = type;
    this.fqn = fqn;
    this.modifiers = modifiers;
    this.multi = multi;
    this.projectID = projectID;
    this.fileID = fileID;
    this.offset = offset;
    this.length = length;
    this.loc = loc;
    this.nwloc = nwloc;
    this.fileType = fileType;
  }
  
  public EntityModel(String fqn, byte[] projectID, byte[] fileID, Byte type) {
    super();
    this.fqn = fqn;
    this.projectID = projectID;
    this.fileID = fileID;
    this.type = type;
  }

  public Byte getType() {
    return type;
  }

  public String getFqn() {
    return fqn;
  }

  public Integer getModifiers() {
    return modifiers;
  }

  public Integer getMulti() {
    return multi;
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

  public Integer getLoc() {
    return loc;
  }

  public Integer getNwloc() {
    return nwloc;
  }

  public Byte getFileType() {
    return fileType;
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

  public void setModifiers(Integer modifiers) {
    this.modifiers = modifiers;
  }

  public void setMulti(Integer multi) {
    this.multi = multi;
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
  }

  public void setLength(Integer length) {
    this.length = length;
  }

  public void setLoc(Integer loc) {
    this.loc = loc;
  }

  public void setNwloc(Integer nwloc) {
    this.nwloc = nwloc;
  }

  public void setFileType(Byte fileType) {
    this.fileType = fileType;
  }

  @Override
  public String toString() {
    return Serialization.byteArrayToHexString(id)
        + "\n  " + StringUtils.rightPad("fqn: ", 16) + fqn
        + "\n  " + StringUtils.rightPad("type: ", 16) + EnumUtil.getEnumByValue(Entity.values(), type)
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {type}) + ")"
        + (projectID != null ? "\n  " + StringUtils.rightPad("projectID: ", 16) + Serialization.byteArrayToHexString(projectID) : "")
        + (fileID != null ? "\n  " + StringUtils.rightPad("fileID: ", 16) + Serialization.byteArrayToHexString(fileID) : "")
        + (fileType != null ? "\n  " + StringUtils.rightPad("fileType: ", 16) + EnumUtil.getEnumByValue(File.values(), fileType)
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {fileType}) + ")" : "")
        + (modifiers != null ? "\n  " + StringUtils.rightPad("modifiers: ", 16) + Modifier.convertToHumanString(Modifier.convertFromInt(modifiers)) : "")
        + (multi != null ? "\n  " + StringUtils.rightPad("multi: ", 16) + Integer.toHexString(multi) : "")
        + (offset != null ? "\n  " + StringUtils.rightPad("offset: ", 16) + offset : "")
        + (length != null ? "\n  " + StringUtils.rightPad("length: ", 16) + length : "")
        + (loc != null ? "\n  " + StringUtils.rightPad("linesOfCode: ", 16) + loc : "")
        + (nwloc != null ? "\n  " + StringUtils.rightPad("nonWhitespaceLinesOfCode: ", 16) + nwloc : "")
        ;
  }

}
