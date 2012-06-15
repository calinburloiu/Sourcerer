package edu.nus.soc.sourcerer.util;

public class HumanReadable {
  
  /**
   * Converts an array of bytes into a hex string representation.
   *  
   * @param bytes
   * @return
   */
  public static String byteArrayToHexString(byte[] bytes) {
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

}
