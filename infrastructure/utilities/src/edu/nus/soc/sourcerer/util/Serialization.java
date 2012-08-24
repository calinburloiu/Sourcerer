package edu.nus.soc.sourcerer.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

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
   * Converts a String which contains hexadecimal escaped bytes (like \x34),
   * as represented in HBase IRB, to a byte array.
   * 
   * Example: String "A\\x975" => byte[] { (byte) 'A', (byte) 0x97, (byte) '5' }
   * 
   * @param escStr
   * @return
   */
  public static byte[] escStringtoByteArray(String strEsc) {
    byte[] buffer = new byte[strEsc.length()];
    int c = 0;
    
    byte[] bEsc = strEsc.toLowerCase().getBytes();
    for (int i = 0; i < bEsc.length; i++) {
      if (bEsc[i] == (byte) '\\' && i+3 <= bEsc.length-1
          && bEsc[i+1] == (byte) 'x'
          && (bEsc[i+2] <= (byte) '9' && bEsc[i+2] >= (byte) '0'
              || bEsc[i+2] <= (byte) 'f' && bEsc[i+2] >= (byte) 'a')
          && (bEsc[i+3] <= (byte) '9' && bEsc[i+3] >= (byte) '0'
              || bEsc[i+3] <= (byte) 'f' && bEsc[i+3] >= (byte) 'a')) {
        buffer[c++] = hexStringToByteArray(new String(bEsc, i+2, 2))[0];
        i += 3;
      }
      else {
        buffer[c++] = bEsc[i];
      }
    }
    
    return Bytes.head(buffer, c);
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
    
    int len = bb.position();
    bb.position(0);
    newBb.put(bb.array(), 0, len);
    
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
  
  /**
   * Generates a byte array where all the bytes are the same.
   * 
   * @param b byte to duplicate
   * @param length length of the output byte array
   * @return
   */
  public static byte[] genPadBytes(byte b, int length) {
    byte[] output = new byte[length];
    for (int i = 0; i < length; i++) {
      output[i] = b;
    }
    
    return output;
  }
  
  public static byte[] incBytes(byte[] src) {
    if (src == null) {
      return null;
    }
    if (src.length == 0) {
      return new byte[] {(byte) 0x00};
    }
    
    if (src[src.length - 1] == (byte) 0xFF) {
      return Bytes.add(src, new byte[] {(byte) 0x00});
    }
    
    byte[] dest = Arrays.copyOf(src, src.length);
    dest[dest.length - 1] = (byte) ( (int) dest[dest.length - 1] + 1 );
    return dest;
  }

  public static void main(String args[]) throws Exception {
    byte[] ar = Bytes.toBytes("Calin-Andrei Burloiu");
//    for (byte b : bytes) {
      System.out.print(Bytes.toString(incBytes(ar)));
//    }
  }
}
