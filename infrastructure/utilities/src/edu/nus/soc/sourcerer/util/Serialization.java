package edu.nus.soc.sourcerer.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;

public class Serialization {
  
  /**
   * Converts an array of bytes into a hex string representation.
   *  
   * @param bytes
   * @return
   */
  public static String byteArrayToHexString(byte[] bytes) {
    if (bytes == null)
      return null;
    
    StringBuffer hexString = new StringBuffer();
    String hex;
    
    for (byte b : bytes) {
      long lb;
      if ((b & 0x80) == 0x00)
        lb = (long)b;
      else
        lb = (long)(0x80 | (0x7f & b));
      hex = Long.toString(lb, 16);
      
      if (hex.length() == 1)
        hexString.append("0");
      
      hexString.append(hex);
    }
    
    return hexString.toString();
  }
  
  /**
   * Converts a hex string to an array of bytes.
   * 
   * The hex string must have an even length. If not the method throws
   * StringSerializationException.
   * 
   * @param hexString
   * @return
   */
  public static byte[] hexStringToByteArray(byte[] hexString) 
      throws StringSerializationException {
    if (hexString == null)
      return null;
    
    int len = hexString.length;
    
    if ((len % 2) != 0) {
      byte[] oldHexString = hexString;
      hexString = Bytes.add(new byte[] {(byte)'0'}, oldHexString);
//      throw new StringSerializationException(
//          "Invalid hash string '" + new String(hexString)
//          + "'; must have an even length.");
    }
    
    byte hb = 0;
    byte[] bytes = new byte[len/2];
    int j = 0;
    
    for (int i = 0; i < len; i++) {
      if (hexString[i] >= 'a' && hexString[i] <= 'z')
        hexString[i] += 'A' - 'a';
      
      if (i % 2 == 0)
        hb = Bytes.toBinaryFromHex(hexString[i]);
      else {
        bytes[j++] = (byte)((hb << 4) | (int)Bytes.toBinaryFromHex(hexString[i]));
      }
    }
    
    return bytes;
  }
  
  /**
   * Converts a hex string to an array of bytes.
   * 
   * The hex string must have an even length. If not the method throws
   * StringSerializationException.
   * 
   * @param hexString
   * @return
   */
  public static byte[] hexStringToByteArray(String hexString) 
      throws StringSerializationException {
    if (hexString == null)
      return null;
    
    try {
      return hexStringToByteArray(
          hexString.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new StringSerializationException(
          "Your platform might not support UTF-8.", e);
    }
  }
  
  /**
   * Returns a new ByteBuffer with the same content and double capacity.
   * 
   * @param bb
   * @return
   */
  public static ByteBuffer reallocateByteBuffer(ByteBuffer bb) {
    int oldCapacity = bb.capacity();
    ByteBuffer newBb = ByteBuffer.allocate(oldCapacity * 2);
    newBb.put(bb);
    
    return newBb;
  }
  
  /**
   * Returns a new Byte buffer with the same content and the capacity equal
   * to input current position.
   * 
   * @param bb
   * @return
   */
  public static byte[] getFitByteBufferBytes(ByteBuffer bb) {
    int length = bb.position();
    byte[] outBytes = new byte[length];
    bb.position(0);
    bb.get(outBytes, 0, length);
    
    return outBytes;
  }

//  public static void main(String args[]) {
//    String s = "abcdefghijklmnop";
//    String hex = null;
//    try {
//      hex = Serialization.byteArrayToHexString(s.getBytes("UTF-8"));
//    } catch (UnsupportedEncodingException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
//    
//    System.out.println("hex: " + hex + "\n" + "back: "
//        + new String(Serialization.hexStringToByteArray(hex)));
//    
//    System.out.println(Bytes.toBinaryFromHex("A".getBytes()[0]));
//  }
}
