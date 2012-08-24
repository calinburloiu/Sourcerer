package edu.nus.soc.sourcerer.model.ddb;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import edu.nus.soc.sourcerer.util.EnumUtil;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;

public class SourcedRelationsModel implements Model {
  
  protected byte[] sourceID = null;
  protected Byte sourceType = null;
  protected Double sourceRank = null;
  protected List<RelationTargetModel> relations = null;
  
  public SourcedRelationsModel() {
    this(null, null, null);
  }
  
  public SourcedRelationsModel(byte[] sourceID, Byte sourceType,
      Double sourceRank) {
    super();
    this.sourceID = sourceID;
    this.sourceType = sourceType;
    this.sourceRank = sourceRank;
    this.relations = new ArrayList<RelationTargetModel>();
  }
  
  public SourcedRelationsModel(byte[] sourceID, Byte sourceType,
      Double sourceRank, int relationsListCapacity) {
    super();
    this.sourceID = sourceID;
    this.sourceType = sourceType;
    this.sourceRank = sourceRank;
    this.relations = new ArrayList<RelationTargetModel>(relationsListCapacity);
  }

  public byte[] getSourceID() {
    return sourceID;
  }

  public void setSourceID(byte[] sourceID) {
    this.sourceID = sourceID;
  }

  public Byte getSourceType() {
    return sourceType;
  }

  public void setSourceType(Byte sourceType) {
    this.sourceType = sourceType;
  }

  public Double getSourceRank() {
    return sourceRank;
  }

  public void setSourceRank(Double rank) {
    this.sourceRank = rank;
  }

  public List<RelationTargetModel> getRelations() {
    return relations;
  }
  
  public void addRelation(RelationTargetModel relationTarget) {
    relations.add(relationTarget);
  }
  
  public int getRelationsCount() {
    return relations.size();
  }

  public static class RelationTargetModel implements Model {
    protected byte[] targetID = null;
    protected Byte targetType = null;
    protected byte[] relationID = null;
    protected Byte relationKind = null;
    
    public RelationTargetModel() {}
    
    public RelationTargetModel(byte[] targetID, Byte targetType,
        byte[] relationID, Byte relationKind) {
      super();
      this.targetID = targetID;
      this.targetType = targetType;
      this.relationID = relationID;
      this.relationKind = relationKind;
    }

    public byte[] getTargetID() {
      return targetID;
    }

    public void setTargetID(byte[] targetID) {
      this.targetID = targetID;
    }

    public Byte getTargetType() {
      return targetType;
    }

    public void setTargetType(Byte targetType) {
      this.targetType = targetType;
    }

    public byte[] getRelationID() {
      return relationID;
    }

    public void setRelationID(byte[] relationID) {
      this.relationID = relationID;
    }

    public Byte getRelationKind() {
      return relationKind;
    }

    public void setRelationKind(Byte relationKind) {
      this.relationKind = relationKind;
    }

    @Override
    public String toString() {
      return "\n    relationID: " + Serialization.byteArrayToHexString(relationID)
          + "\n      " + StringUtils.rightPad("relationType: ", 18) + EnumUtil.getEnumByValue(Relation.values(), RelationModel.kindToType(relationKind))
              + "(0x" + Serialization.byteArrayToHexString(new byte[] {RelationModel.kindToType(relationKind)}) + ")"
          + "\n      " + StringUtils.rightPad("relationClass: ", 18) + EnumUtil.getEnumByValue(RelationClass.values(), RelationModel.kindToClass(relationKind))
              + "(0x" + Serialization.byteArrayToHexString(new byte[] {RelationModel.kindToClass(relationKind)}) + ")"
          + "\n      " + StringUtils.rightPad("targetEntityID: ", 18) + Serialization.byteArrayToHexString(targetID)
          + "\n      " + StringUtils.rightPad("targetEntityType: ", 18) + EnumUtil.getEnumByValue(Entity.values(), targetType)
              + "(0x" + Serialization.byteArrayToHexString(new byte[] {targetType}) + ")";
    }
  }

  @Override
  public String toString() {
    String str = "sourceEntityID: " + Serialization.byteArrayToHexString(sourceID)
        + "\n  " + StringUtils.rightPad("sourceType: ", 16) + EnumUtil.getEnumByValue(Entity.values(), sourceType)
            + "(0x" + Serialization.byteArrayToHexString(new byte[] {sourceType}) + ")"
        + (sourceRank != null ? "\n  " + StringUtils.rightPad("sourceCodeRank: ", 16) + sourceRank : "")
        + "\n  " + StringUtils.rightPad("relationsCount: ", 16) + relations.size()
        + "\n  " + StringUtils.rightPad("relations: ", 16)
        ;
    
    if (!relations.isEmpty()) {
      for (RelationTargetModel relation : relations) {
        str += relation.toString();
      }
    }
    else {
      str += "[None]";
    }
    
    return str;
  }
  
  
}
