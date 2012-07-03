package edu.nus.soc.sourcerer.model.ddb;

import edu.uci.ics.sourcerer.model.File;


/**
 * Default model for HBase `files` table.
 * 
 * @author Calin-Andrei Burloiu
 * 
 */
public class FileModel extends ModelWithID {
  protected File type;
  protected byte[] projectID;
  
  // Meta
  protected String name;
  protected String path;
  protected byte[] hash;
  protected byte[] jarProjectID;
  
  // Metrics
  protected Integer loc;
  protected Integer nwloc;
  
  public FileModel(byte[] fileID, File type, byte[] projectID, String name,
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

  public FileModel(File type, byte[] projectID, String name, String path,
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
    if (type == File.JAR) {
      id = computeId(1024, "name");
    } else {
      id = computeId(1024, "path");
    }
  }
  
  public FileModel(File type, String name, String path) {
    this(type, null, name, path, null, null, null, null);
  }

  public File getType() {
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
