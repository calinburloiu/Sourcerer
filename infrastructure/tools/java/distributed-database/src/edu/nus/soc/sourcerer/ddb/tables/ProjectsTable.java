package edu.nus.soc.sourcerer.ddb.tables;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.uci.ics.sourcerer.model.Project;

/**
 * This class contains information about HBase projects table including
 * table name, column families and column qualifiers name and a static method
 * which retrieves an HTableDescriptor.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class ProjectsTable {
  public static String NAME = "projects";
  
  public static byte[] CF_DEFAULT = Bytes.toBytes("d");
  public static byte[] COL_NAME = Bytes.toBytes("name");
  public static byte[] COL_DESCRIPTION = Bytes.toBytes("dscr");
  public static byte[] COL_VERSION = Bytes.toBytes("ver");
  public static byte[] COL_GROUP = Bytes.toBytes("grp");
  public static byte[] COL_PATH = Bytes.toBytes("path");
  public static byte[] COL_HASSOURCE = Bytes.toBytes("hsrc");
  
  public static byte[] CF_METRICS = Bytes.toBytes("m");
  public static byte[] COL_METRIC_LOC = Bytes.toBytes("loc");
  public static byte[] COL_METRIC_NWLOC = Bytes.toBytes("nwloc");
  
  /**
   * Compute row key.
   * 
   * @param type
   * @param projectID
   * @return
   */
  public static byte[] row(Project type, byte[] projectID) {
    return Bytes.add(new byte[] {type.getValue()}, projectID);
  }
  
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
