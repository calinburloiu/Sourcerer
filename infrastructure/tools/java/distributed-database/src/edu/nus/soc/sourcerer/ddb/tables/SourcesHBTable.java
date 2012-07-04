package edu.nus.soc.sourcerer.ddb.tables;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;

public class SourcesHBTable extends HBTable {
  private static SourcesHBTable instance = null;
  
  public static final String NAME = "sources";
  
  public static final byte[] CF_DEFAULT = Bytes.toBytes("d");
  public static final byte[] COL_CONTENT = Bytes.toBytes("cnt");

  private SourcesHBTable() {
    super();
  }
  
  public static SourcesHBTable getInstance() {
    if (instance == null) {
      instance = new SourcesHBTable();
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
    
    return tableDesc;
  }
}
