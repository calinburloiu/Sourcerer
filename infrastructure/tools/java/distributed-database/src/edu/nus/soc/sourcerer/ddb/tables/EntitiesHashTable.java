package edu.nus.soc.sourcerer.ddb.tables;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;

/**
 * This class contains information about HBase entities hash table including
 * table name, column families and column qualifiers name and a static method
 * which retrieves an HTableDescriptor.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class EntitiesHashTable {
  public static String NAME = "entities_hash";
  
  public static byte[] CF_DEFAULT = Bytes.toBytes("d");
  public static byte[] COL_ENTITYTYPE = Bytes.toBytes("et");
  public static byte[] COL_MODIFIERS = Bytes.toBytes("modf");
  public static byte[] COL_MULTI = Bytes.toBytes("mult");
  public static byte[] COL_PID = Bytes.toBytes("pid");
  public static byte[] COL_FID = Bytes.toBytes("fid");
  public static byte[] COL_OFFSET = Bytes.toBytes("offs");
  public static byte[] COL_LENGTH = Bytes.toBytes("len");
  
  public static byte[] CF_METRICS = Bytes.toBytes("m");
  public static byte[] COL_METRIC_LOC = Bytes.toBytes("loc");
  public static byte[] COL_METRIC_NWLOC = Bytes.toBytes("nwloc");

  public static HTableDescriptor getTableDescriptor(
      DatabaseConfiguration dbConf) {
    HTableDescriptor tableDesc = new HTableDescriptor(
        dbConf.getTablePrefix() + NAME);
    
    // Default column family
    HColumnDescriptor cfDefault = new HColumnDescriptor(CF_DEFAULT);
    cfDefault.setMaxVersions(3);
    cfDefault.setCompressionType(dbConf.getDefaultCompression());
    cfDefault.setBlockCacheEnabled(false);
    cfDefault.setBloomFilterType(StoreFile.BloomType.ROW);
    tableDesc.addFamily(cfDefault);
    
    // Metrics column family
    HColumnDescriptor cfMetrics = new HColumnDescriptor(CF_METRICS);
    cfMetrics.setMaxVersions(3);
    cfMetrics.setCompressionType(Compression.Algorithm.NONE);
    cfMetrics.setBlockCacheEnabled(false);
    cfMetrics.setBloomFilterType(StoreFile.BloomType.NONE);
    tableDesc.addFamily(cfMetrics);
    
    return tableDesc;
  }

}
