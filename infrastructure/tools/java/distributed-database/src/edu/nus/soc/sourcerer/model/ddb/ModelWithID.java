package edu.nus.soc.sourcerer.model.ddb;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import edu.nus.soc.sourcerer.util.StringSerializationException;

/**
 * A base class for models that have an ID computed from some of their fields.
 * The ID is computed as an MD5 hash.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class ModelWithID {
  protected byte[] id;
  
  /**
   * Computes MD5 hash for the byte representation of the provided fields.
   * Network byte order is used.
   * The fields can only be primitives or Strings.
   * 
   * @param inputBytesCount the expected maximum number of bytes from which the
   * hash is computed
   * @param fields
   */
  public byte[] computeId(int inputBytesCount, Field ... fields) {
    ByteBuffer bb = ByteBuffer.allocate(inputBytesCount);
    
    for (Field field : fields) {
      try {
        if (field.getType() == String.class) {
          String strField = (String)field.get(this);
          try {
            bb.put(strField.getBytes("UTF-8"));
          } catch (UnsupportedEncodingException e) {
            throw new StringSerializationException(
                "Your platform might not support UTF-8.", e);
          }
        }
        else if (field.getType() == Byte.TYPE) {
          bb.put(field.getByte(this));
        }
        else if (field.getType() == Short.TYPE) {
          bb.putShort(field.getShort(this));
        }
        else if (field.getType() == Integer.TYPE) {
          bb.putInt(field.getInt(this));
        }
        else if (field.getType() == Long.TYPE) {
          bb.putLong(field.getLong(this));
        }
        else if (field.getType() == Float.TYPE) {
          bb.putFloat(field.getFloat(this));
        }
        else if (field.getType() == Double.TYPE) {
          bb.putDouble(field.getDouble(this));
        }
        else if (field.getType() == Boolean.TYPE) {
          bb.put((byte)(field.getBoolean(this) ? 0x01 : 0x00));
        }
        else if (field.getType() == Character.TYPE) {
          bb.putChar(field.getChar(this));
        }
      } catch (IllegalAccessException e) {
        throw new Error("Reflection programming error.", e);
      } 
    }
    
    int length = bb.position();
    byte[] inputBytes = new byte[length];
    bb.position(0);
    bb.get(inputBytes, 0, length);
    
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

}
