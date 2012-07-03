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
  public static final byte[] COL_OFFSET = Bytes.toBytes("offs");
  public static final byte[] COL_LENGTH = Bytes.toBytes("len");
  
  public static final byte[] CF_METRICS = Bytes.toBytes("m");
  public static final byte[] COL_METRIC_LOC = Bytes.toBytes("loc");
  public static final byte[] COL_METRIC_NWLOC = Bytes.toBytes("nwloc");
  
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
    
    return tableDesc;
  }

}
