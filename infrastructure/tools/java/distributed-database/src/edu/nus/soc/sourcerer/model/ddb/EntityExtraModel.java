package edu.nus.soc.sourcerer.model.ddb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class EntityExtraModel implements Model {

  protected Integer modifiers;
  protected Integer multi;
  protected Integer offset;
  protected Integer length;
  
  /** Model size in bytes */
  public static int SIZE = 16;
  
  public EntityExtraModel(Integer modifiers, Integer multi,
      Integer offset, Integer length) {
    super();
    this.modifiers = modifiers;
    this.multi = multi;
    this.offset = offset;
    this.length = length;
  }
  
  public static List<EntityExtraModel> getExtraFromBytes(byte[] src) {
    if (src.length % SIZE != 0)
      throw new RuntimeException("Extra bytes array is not a multiple of " + SIZE);
    
    int count = src.length / SIZE;
    List<EntityExtraModel> extra =
        new ArrayList<EntityExtraModel>(src.length / SIZE);
    ByteBuffer bb = ByteBuffer.wrap(src);
    int offset = 0, length = 0, modifiers = 0 , multi = 0;
    
    for (int i=0; i<count; i++) {
      modifiers = bb.getInt();
      multi = bb.getInt();
      offset = bb.getInt();
      length = bb.getInt();
      extra.add(new EntityExtraModel(modifiers, multi, offset, length));
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
  
  public Integer getModifiers() {
    return modifiers;
  }

  public void setModifiers(Integer modifiers) {
    this.modifiers = modifiers;
  }

  public Integer getMulti() {
    return multi;
  }

  public void setMulti(Integer multi) {
    this.multi = multi;
  }

  @Override
  public String toString() {
    return "\n      offset:    " + offset
         + "\n      length:    " + length
         + "\n      modifiers: " + length
         + "\n      multi:     " + length;
  }
}
