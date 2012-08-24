package edu.nus.soc.sourcerer.ddb.tables;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;

/**
 * This class contains information about HBase relations_hash table including
 * table name, column families and column qualifiers name and a static method
 * which retrieves an HTableDescriptor.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class RelationsHashHBTable extends HBTable {
  private static RelationsHashHBTable instance = null;
  
  public static final String NAME = "relations_hash";
  
  public static final byte[] CF_DEFAULT = Bytes.toBytes("d");
  public static final byte[] COL_KIND = Bytes.toBytes("kind");
  public static final byte[] COL_SOURCE_ID = Bytes.toBytes("seid");
  public static final byte[] COL_SOURCE_TYPE = Bytes.toBytes("set");
  public static final byte[] COL_TARGET_ID = Bytes.toBytes("teid");
  public static final byte[] COL_TARGET_TYPE = Bytes.toBytes("tet");
  public static final byte[] COL_PROJECT_ID = Bytes.toBytes("pid");
  public static final byte[] COL_FILE_ID = Bytes.toBytes("fid");
  public static final byte[] COL_FILE_TYPE = Bytes.toBytes("ft");
  public static final byte[] COL_OFFSET = Bytes.toBytes("offs");
  public static final byte[] COL_LENGTH = Bytes.toBytes("len");
  
  private RelationsHashHBTable() {
    super();
  }
  
  public static RelationsHashHBTable getInstance() {
    if (instance == null) {
      instance = new RelationsHashHBTable();
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
