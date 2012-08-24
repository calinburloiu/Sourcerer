package edu.nus.soc.sourcerer.model.ddb;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import edu.nus.soc.sourcerer.util.Serialization;
import edu.nus.soc.sourcerer.util.StringSerializationException;

/**
 * A base class for models that have an ID computed from some of their fields.
 * The ID is computed as an MD5 hash.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class ModelWithID implements Model {
  protected byte[] id;
  
  public ModelWithID() {
    super();
    this.id = null;
  }
  
  public ModelWithID(byte[] id) {
    super();
    this.id = id;
  }

  /**
   * Computes MD5 hash for the byte representation of the provided fields.
   * Network byte order is used.
   * The fields can only be primitives or Strings.
   * 
   * @param inputBytesCount the expected maximum number of bytes from which the
   * hash is computed
   * @param fields
   */
  public byte[] computeId(int inputBytesCount, String ... fieldNames) {
    ByteBuffer bb = ByteBuffer.allocate(inputBytesCount);
    Field field;
    
    for (String fieldName : fieldNames) {
      try {
        field = this.getClass().getDeclaredField(fieldName);
      } catch (SecurityException e) {
        throw new Error("Reflection programming error.", e);
      } catch (NoSuchFieldException e) {
        throw new Error("Reflection programming error.", e);
      }
      
      try {
        if (field.get(this) == null)
          continue;
        
        if (field.getType() == String.class) {
          String strField = (String)field.get(this);
          try {
            bb.put(strField.getBytes("UTF-8"));
          } catch (UnsupportedEncodingException e) {
            throw new StringSerializationException(
                "Your platform might not support UTF-8.", e);
          }
        }
        else if (field.get(this) != null && field.getType() == Byte.class) {
          bb.put((Byte)field.get(this));
        }
        else if (field.get(this) != null && field.getType() == Short.class) {
          bb.putShort((Short)field.get(this));
        }
        else if (field.get(this) != null && field.getType() == Integer.class) {
          bb.putInt((Integer)field.get(this));
        }
        else if (field.get(this) != null && field.getType() == Long.class) {
          bb.putLong((Long)field.get(this));
        }
        else if (field.get(this) != null && field.getType() == Float.class) {
          bb.putFloat((Float)field.get(this));
        }
        else if (field.get(this) != null && field.getType() == Double.class) {
          bb.putDouble((Double)field.get(this));
        }
        else if (field.get(this) != null && field.getType() == Boolean.class) {
          bb.put((byte)((Boolean)field.get(this) ? 0x01 : 0x00));
        }
        else if (field.get(this) != null && field.getType() == Character.class) {
          bb.putChar((Character)field.get(this));
        }
        else if (field.get(this) != null && field.getType().isArray()
            && field.getType().getComponentType() == Byte.TYPE) {
          byte[] byteArray = (byte[])field.get(this);
          bb.put(byteArray);
        }
      } catch (IllegalAccessException e) {
        throw new Error("Reflection programming error.", e);
      } 
    }
    
    byte[] inputBytes = Serialization.getFitByteBufferBytes(bb);
    
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 algorithm is not available.", e);
    }
    
    return md.digest(inputBytes);
  }

  public byte[] getId() {
    return id;
  }

  public void setId(byte[] id) {
    this.id = id;
  }
  
//  public static void main(String[] args) throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
//    ModelWithID m = new ModelWithID(new byte[] {'C', 'a', 'B'});
//    
//    Field field = m.getClass().getDeclaredField("id");
//    System.out.println("Type: " + field.getType().toString() + "\nIsArray: "
//        + field.getType().isArray() + "\nComponentType: " + field.getType().getComponentType());
//    
//    byte[] id2 = (byte[])field.get(m);
//    System.out.println(new String(id2));
//  }

}
