package edu.nus.soc.sourcerer.ddb.tables;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;

/**
 * This class contains information about HBase inverse relations table including
 * table name, column families and column qualifiers name and a static method
 * which retrieves an HTableDescriptor.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class RelationsInverseHBTable extends HBTable {
  private static RelationsInverseHBTable instance = null;
  
  public static final String NAME = "relations_inverse";
  
  public static final byte[] CF_DEFAULT = Bytes.toBytes("d");
  
  private RelationsInverseHBTable() {
    super();
  }
  
  public static RelationsInverseHBTable getInstance() {
    if (instance == null) {
      instance = new RelationsInverseHBTable();
    }
    return instance;
  }
  
  @Override
  public String getName() {
    return NAME;
  }
  
  @Override
  public void setupHTable() {
    super.setupHTable();
    
    hTable.setScannerCaching(128);
  }
  
  /**
   * Compute row key.
   * 
   * @param targetEntity
   * @param relationType
   * @param sourceEntity
   * @return
   */
  public static byte[] row(byte[] targetEntity, Relation relationType,
      RelationClass relationClass, byte[] sourceEntity) {
    return Bytes.add(targetEntity,
        new byte[] {(byte) (relationType.getValue() | relationClass.getValue())},
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
  
  public static HTableDescriptor getTableDescriptor() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    
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
