package edu.nus.soc.sourcerer.ddb.tables;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.uci.ics.sourcerer.model.Relation;

/**
 * This class contains information about HBase inverse relations table including
 * table name, column families and column qualifiers name and a static method
 * which retrieves an HTableDescriptor.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class InverseRelationsTable {
  public static String NAME = "inverse_relations";
  
  public static byte[] CF_DEFAULT = Bytes.toBytes("d");
  
  /**
   * Compute row key.
   * 
   * @param targetEntity
   * @param relationType
   * @param sourceEntity
   * @return
   */
  public static byte[] row(byte[] targetEntity, Relation relationType,
      byte[] sourceEntity) {
    return Bytes.add(targetEntity, new byte[] {relationType.getValue()}, 
        sourceEntity);
  }
  
  /**
   * Compute column qualifier for the default column family.
   * 
   * @param projectID
   * @param fileID
   * @return
   */
  public static byte[] col(byte[] projectID, byte[] fileID) {
    return Bytes.add(projectID, fileID);
  }
  
  public static HTableDescriptor getTableDescriptor(
      DatabaseConfiguration dbConf) {
    HTableDescriptor tableDesc = new HTableDescriptor(
        dbConf.getTablePrefix() + NAME);
    
    // Default column family
    HColumnDescriptor cfDefault = new HColumnDescriptor(CF_DEFAULT);
    cfDefault.setMaxVersions(3);
    cfDefault.setCompressionType(Compression.Algorithm.NONE);
    cfDefault.setBlockCacheEnabled(false);
    cfDefault.setBloomFilterType(StoreFile.BloomType.ROW);
    tableDesc.addFamily(cfDefault);
    
    return tableDesc;
  }

}
