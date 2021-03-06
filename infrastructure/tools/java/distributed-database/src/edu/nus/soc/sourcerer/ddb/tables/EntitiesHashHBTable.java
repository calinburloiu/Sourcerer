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
public class EntitiesHashHBTable extends HBTable {
  private static EntitiesHashHBTable instance = null;
  
  public static final String NAME = "entities_hash";
  
  public static final byte[] CF_DEFAULT = Bytes.toBytes("d");
  public static final byte[] COL_ENTITYTYPE = Bytes.toBytes("et");
  public static final byte[] COL_FQN = Bytes.toBytes("fqn");
  public static final byte[] COL_MODIFIERS = Bytes.toBytes("modf");
  public static final byte[] COL_MULTI = Bytes.toBytes("mult");
  public static final byte[] COL_PID = Bytes.toBytes("pid");
  public static final byte[] COL_FID = Bytes.toBytes("fid");
  public static final byte[] COL_FILETYPE = Bytes.toBytes("ft");
  public static final byte[] COL_OFFSET = Bytes.toBytes("offs");
  public static final byte[] COL_LENGTH = Bytes.toBytes("len");
  
  public static final byte[] CF_METRICS = Bytes.toBytes("m");
  public static final byte[] COL_METRIC_LOC = Bytes.toBytes("loc");
  public static final byte[] COL_METRIC_NWLOC = Bytes.toBytes("nwloc");
  
  public static final byte[] CF_RELATIONS = Bytes.toBytes("r");
  public static final byte[] COL_RELATIONS_SOURCE_TYPE = Bytes.toBytes("set");
  public static final byte[] COL_RELATIONS_SOURCE_RANK = Bytes.toBytes("rank");
  public static final byte[] COL_RELATIONS_TARGETS_COUNT = Bytes.toBytes("tec");
  public static final byte[] COL_RELATIONS_TARGETS = Bytes.toBytes("te");
  public static final byte[] COL_RELATIONS = Bytes.toBytes("rids");
  
  public static final byte[] DANGLING_CACHE_START_ROW = new byte[] {
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
  };
  
  private EntitiesHashHBTable() {
    super();
  }
  
  public static EntitiesHashHBTable getInstance() {
    if (instance == null) {
      instance = new EntitiesHashHBTable();
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
    
    // Relations and CodeRank column family
    HColumnDescriptor cfRelations = new HColumnDescriptor(CF_RELATIONS);
    cfRelations.setMaxVersions(3);
    cfRelations.setCompressionType(Compression.Algorithm.NONE);
    cfRelations.setBlockCacheEnabled(false);
    cfRelations.setBloomFilterType(StoreFile.BloomType.ROW);
    tableDesc.addFamily(cfRelations);
    
    return tableDesc;
  }

}
