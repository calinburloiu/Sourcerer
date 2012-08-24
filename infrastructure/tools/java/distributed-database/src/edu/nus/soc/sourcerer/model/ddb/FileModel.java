package edu.nus.soc.sourcerer.model.ddb;

import org.apache.commons.lang.StringUtils;

import edu.nus.soc.sourcerer.util.EnumUtil;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.File;


/**
 * Default model for HBase `files` table.
 * 
 * @author Calin-Andrei Burloiu
 * 
 */
public class FileModel extends ModelWithID {
  protected Byte type;
  protected byte[] projectID;
  
  // Meta
  protected String name;
  protected String path;
  protected byte[] hash;
  protected byte[] jarProjectID;
  
  // Metrics
  protected Integer loc;
  protected Integer nwloc;
  
  public FileModel(byte[] fileID, Byte type, byte[] projectID, String name,
      String path, byte[] hash, byte[] jarProjectID, Integer loc,
      Integer nwloc) {
    super(fileID);
    this.type = type;
    this.projectID = projectID;
    this.name = name;
    this.path = path;
    this.hash = hash;
    this.jarProjectID = jarProjectID;
    this.loc = loc;
    this.nwloc = nwloc;
  }

  public FileModel(Byte type, byte[] projectID, String name, String path,
      byte[] hash, byte[] jarProjectID, Integer loc, Integer nwloc) {
    super();
    this.type = type;
    this.projectID = projectID;
    this.name = name;
    this.path = path;
    this.hash = hash;
    this.jarProjectID = jarProjectID;
    this.loc = loc;
    this.nwloc = nwloc;
    
    // Compute ID.
    if (type == File.JAR.getValue()) {
      id = computeId(1024, "name");
    } else {
      id = computeId(1024, "path");
    }
  }
  
  public FileModel(Byte type, String name, String path) {
    this(type, null, name, path, null, null, null, null);
  }
  
  @Override
  public String toString() {
    return Serialization.byteArrayToHexString(id)
        + "\n  " + StringUtils.rightPad("name: ", 16) + name
        + (path != null ? "\n  " + StringUtils.rightPad("path: ", 16) + path : "")
        + "\n  " + StringUtils.rightPad("type: ", 16) + EnumUtil.getEnumByValue(File.values(), type)
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {type}) + ")"
        + "\n  " + StringUtils.rightPad("projectID: ", 16) + Serialization.byteArrayToHexString(projectID)
        + (hash != null ? "\n  " + StringUtils.rightPad("hash: ", 16) + Serialization.byteArrayToHexString(hash) : "")
        + (jarProjectID != null ? "\n  " + StringUtils.rightPad("jarProjectID: ", 16) + Serialization.byteArrayToHexString(jarProjectID) : "")
        + (loc != null ? "\n  " + StringUtils.rightPad("loc: ", 16) + loc : "")
        + (nwloc != null ? "\n  " + StringUtils.rightPad("nwloc: ", 16) + nwloc : "");
  }

  public Byte getType() {
    return type;
  }

  public byte[] getProjectID() {
    return projectID;
  }

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }

  public byte[] getHash() {
    return hash;
  }

  public byte[] getJarProjectID() {
    return jarProjectID;
  }

  public Integer getLoc() {
    return loc;
  }

  public Integer getNwloc() {
    return nwloc;
  }
  
  
}
