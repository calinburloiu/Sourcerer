package edu.nus.soc.sourcerer.ddb.tables;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.util.StringSerializationException;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.File;
import edu.uci.ics.sourcerer.model.Relation;

/**
 * This class contains information about HBase files table including
 * table name, column families and column qualifiers name and a static method
 * which retrieves an HTableDescriptor.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class FilesTable {
  public static String NAME = "files";
  
  public static byte[] CF_DEFAULT = Bytes.toBytes("d");
  public static byte[] COL_NAME = Bytes.toBytes("name");
  public static byte[] COL_PATH = Bytes.toBytes("path");
  public static byte[] COL_HASH = Bytes.toBytes("hash");
  public static byte[] COL_JARPID = Bytes.toBytes("jpid");
  
  public static byte[] CF_METRICS = Bytes.toBytes("m");
  public static byte[] COL_METRIC_LOC = Bytes.toBytes("loc");
  public static byte[] COL_METRIC_NWLOC = Bytes.toBytes("nwloc");
  
  public static byte[] CF_ENTITIES = Bytes.toBytes("e");
  
  public static byte[] CF_RELATIONS = Bytes.toBytes("r");
  
  public static byte[] CF_IMPORTS = Bytes.toBytes("i");
  
  /**
   * Compute row key.
   * 
   * @param projectID
   * @param fileType
   * @param fileID
   * @return
   */
  public static byte[] row(byte[] projectID, File fileType, byte[] fileID) {
    return Bytes.add(projectID, new byte[] {fileType.getValue()}, fileID);
  }
  
  /**
   * Compute column qualifier for the entities column family.
   * 
   * @param entityType
   * @param fqn
   * @return
   */
  public static byte[] entityCol(Entity entityType, String fqn) {
    try {
      return Bytes.add(new byte[] {entityType.getValue()}, fqn.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new StringSerializationException(e);
    }
  }
  
  /**
   * Compute column qualifier for the relations column family.
   * 
   * @param relationType
   * @param targetEntity
   * @param sourceEntity
   * @return
   */
  public static byte[] relationCol(Relation relationType, byte[] targetEntity,
      byte[] sourceEntity) {
    return Bytes.add(new byte[] {relationType.getValue()}, targetEntity,
        sourceEntity);
  }
  
  /**
   * Compute column qualifier for the imports column family.
   * 
   * @param fqn
   * @return
   */
  public static byte[] importCol(String fqn) {
    try {
      return fqn.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new StringSerializationException(e);
    }
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
    
    // Entities column family
    HColumnDescriptor cfEntities = new HColumnDescriptor(CF_ENTITIES);
    cfEntities.setMaxVersions(3);
    cfEntities.setCompressionType(dbConf.getDefaultCompression());
    cfEntities.setBlockCacheEnabled(false);
    cfEntities.setBloomFilterType(StoreFile.BloomType.ROW);
    tableDesc.addFamily(cfEntities);
    
    // Relations column family
    HColumnDescriptor cfRelations = new HColumnDescriptor(CF_RELATIONS);
    cfRelations.setMaxVersions(3);
    cfRelations.setCompressionType(Compression.Algorithm.NONE);
    cfRelations.setBlockCacheEnabled(false);
    cfRelations.setBloomFilterType(StoreFile.BloomType.ROW);
    tableDesc.addFamily(cfRelations);
    
    // Imports column family
    HColumnDescriptor cfImports = new HColumnDescriptor(CF_IMPORTS);
    cfImports.setMaxVersions(3);
    cfImports.setCompressionType(Compression.Algorithm.NONE);
    cfImports.setBlockCacheEnabled(false);
    cfImports.setBloomFilterType(StoreFile.BloomType.ROW);
    tableDesc.addFamily(cfImports);
    
    return tableDesc;
  }
  
}
