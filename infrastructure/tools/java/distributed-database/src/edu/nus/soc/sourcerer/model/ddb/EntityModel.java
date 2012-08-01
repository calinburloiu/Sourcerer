package edu.nus.soc.sourcerer.model.ddb;


public class EntityModel extends ModelWithID {
  protected Byte type;
  protected String fqn;
  protected byte[] projectID;
  protected byte[] fileID;
  protected Integer modifiers;
  protected Integer multi;
  protected Integer offset;
  protected Integer length;
  
  // Metrics
  protected Integer loc;
  protected Integer nwloc;
  
  // Extra
  protected Byte fileType;
  
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

}
