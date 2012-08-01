package edu.nus.soc.sourcerer.model.ddb;

public class RelationModel implements Model {
  protected Byte type;
  protected Byte relationClass;
  protected byte[] sourceID;
  protected byte[] targetID;
  protected byte[] projectID;
  protected byte[] fileID;
  protected Integer offset;
  protected Integer length;
  
  // Extra
  protected Byte fileType;
  
  public RelationModel(Byte type, Byte relationClass, byte[] sourceID, byte[] targetID, byte[] projectID, byte[] fileID, Integer offset, Integer length) {
    super();
    this.type = type;
    this.relationClass = relationClass;
    this.sourceID = sourceID;
    this.targetID = targetID;
    this.projectID = projectID;
    this.fileID = fileID;
    this.offset = offset;
    this.length = length;
  }
  
  public RelationModel(Byte type, Byte relationClass, byte[] sourceID,
      byte[] targetID, byte[] projectID, byte[] fileID,
      Integer offset, Integer length, Byte fileType) {
    super();
    this.type = type;
    this.relationClass = relationClass;
    this.sourceID = sourceID;
    this.targetID = targetID;
    this.projectID = projectID;
    this.fileID = fileID;
    this.offset = offset;
    this.length = length;
    this.fileType = fileType;
  }

  public Byte getType() {
    return type;
  }

  public Byte getRelationClass() {
    return relationClass;
  }
  
  /**
   * In HBase both the type and the class of the relation are encoded in 1 byte.
   * The 3 most significant bits are used for class and the rest of them for
   * type.
   * 
   * @return
   */
  public byte getRelationKind() {
    return (byte)(type | relationClass);
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

  public Byte getFileType() {
    return fileType;
  }

}
