package edu.nus.soc.sourcerer.model.ddb;

public class RelationModel extends ModelWithID {
  protected Byte kind;
  protected byte[] sourceID;
  protected byte[] targetID;
  protected byte[] projectID;
  protected byte[] fileID;
  protected Integer offset;
  protected Integer length;
  
  // Extra
  protected Byte sourceType;
  protected Byte targetType;
  protected Byte fileType;
  
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

  public Byte getType() {
    return (byte) (kind & 0x1f);
  }

  public Byte getRelationClass() {
    return (byte) (kind & 0xe0);
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

}
