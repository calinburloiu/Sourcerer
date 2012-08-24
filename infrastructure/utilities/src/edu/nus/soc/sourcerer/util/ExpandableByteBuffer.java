package edu.nus.soc.sourcerer.util;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class ExpandableByteBuffer {
  ByteBuffer byteBuffer = null;

  public ExpandableByteBuffer(int initCapac) {
    byteBuffer = ByteBuffer.allocate(initCapac);
  }
  
  public void put(byte[] src, int offset, int length) {
    do {
      try {
        byteBuffer.put(src, offset, length);
        break;
      } catch (BufferOverflowException e) {
        byteBuffer = Serialization.reallocateByteBuffer(byteBuffer);
      }
    } while (true);
  }
  
  public void put(byte[] src) {
    put(src, 0, src.length);
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }
  
  public byte[] getBytes() {
    return byteBuffer.array();
  }
  
  public byte[] getFitBytes() {
    return Serialization.getFitByteBufferBytes(byteBuffer);
  }

  public int getCapacity() {
    return byteBuffer.capacity();
  }
  
//  public static void main(String args[]) {
//    int capac = 3;
//
//    ExpandableByteBuffer ebb = new ExpandableByteBuffer(3);
//    
//    for (int i=1; i<=capac; i++) {
//      ebb.put(new byte[] {0x01, 0x02, 0x03, 0x04, 0x05}, 1, 4);
//    }
//    
//    System.out.println(ebb.getCapacity());
//    System.out.println(Serialization.byteArrayToHexString(ebb.getBytes()));
//    System.out.println(Serialization.byteArrayToHexString(ebb.getFitBytes()));
//  }
}
