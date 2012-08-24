package edu.nus.soc.sourcerer.ddb.tools;

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;
import edu.nus.soc.sourcerer.util.Serialization;

public class Test {
  
  public static void test() throws Exception {
    byte[] ar = Bytes.toBytes("Calin-Andrei Burloiu");
//    byte[] andrei = Bytes.toBytes("Ana-Maria are mere!!");
    byte[] andrei = new byte[6];
//    Bytes.writeByteArray(andrei, 0, ar, 6, 6);
    System.arraycopy(ar, 6, andrei, 0, 6);
    
    System.out.println(Bytes.toString(andrei));
  }
  
  public static void main(String args[]) throws Exception {
    test();
  }
}
