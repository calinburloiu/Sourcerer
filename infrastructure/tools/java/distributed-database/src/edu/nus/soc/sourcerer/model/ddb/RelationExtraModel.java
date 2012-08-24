package edu.nus.soc.sourcerer.model.ddb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RelationExtraModel implements Model {

  protected Integer offset;
  protected Integer length;
  
  public static int SIZE = 8;
  
  public RelationExtraModel(Integer offset, Integer length) {
    super();
    this.offset = offset;
    this.length = length;
  }
  
  public static List<RelationExtraModel> getExtraFromBytes(byte[] src) {
    if (src.length % SIZE != 0)
      throw new RuntimeException("Relations extra bytes array is not a multiple of " + SIZE);
    
    int count = src.length / SIZE;
    List<RelationExtraModel> extra =
        new ArrayList<RelationExtraModel>(src.length / SIZE);
    ByteBuffer bb = ByteBuffer.wrap(src);
    int offset = 0, length = 0;
    
    for (int i=0; i<count; i++) {
      offset = bb.getInt();
      length = bb.getInt();
      extra.add(new RelationExtraModel(offset, length));
    }
    
    return extra;
  }

  public Integer getOffset() {
    return offset;
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
  }

  public Integer getLength() {
    return length;
  }

  public void setLength(Integer length) {
    this.length = length;
  }

  @Override
  public String toString() {
    return "\n      offset: " + offset
        + "\n      length: " + length;
  }
}
