package edu.nus.soc.sourcerer.model.ddb;

import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;

public class RelationModel implements Model {
  protected Relation type;
  protected RelationClass relationClass;
  protected byte[] sourceID;
  protected byte[] targetID;
  protected byte[] projectID;
  protected byte[] fileID;
  protected Integer offset;
  protected Integer length;
  
  public RelationModel(Relation type, RelationClass relationClass, byte[] sourceID, byte[] targetID, byte[] projectID, byte[] fileID, Integer offset, Integer length) {
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

  public Relation getType() {
    return type;
  }

  public RelationClass getRelationClass() {
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
    return (byte)(type.getValue() | relationClass.getValue());
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
    
}
