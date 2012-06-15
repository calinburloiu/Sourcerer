package edu.nus.soc.sourcerer.ddb.tools;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashTable;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesTable;
import edu.nus.soc.sourcerer.ddb.tables.FilesTable;
import edu.nus.soc.sourcerer.ddb.tables.InverseRelationsTable;
import edu.nus.soc.sourcerer.ddb.tables.ProjectsTable;

import static edu.uci.ics.sourcerer.util.io.Logging.logger;

/**
 * This class is responsible with initialisation operations which prepare the
 * database to used. This operations include the creation of the tables.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class DatabaseInitializer {
  
  protected DatabaseConfiguration databaseConfiguration;
  protected Vector<HTableDescriptor> tables = new Vector<HTableDescriptor>(16);
  
  public DatabaseInitializer(DatabaseConfiguration databaseConfiguration) {
    this.databaseConfiguration = databaseConfiguration; 
    
    tables.add(ProjectsTable.getTableDescriptor(databaseConfiguration));
    tables.add(FilesTable.getTableDescriptor(databaseConfiguration));
    tables.add(EntitiesTable.getTableDescriptor(databaseConfiguration));
    tables.add(EntitiesHashTable.getTableDescriptor(databaseConfiguration));
    tables.add(InverseRelationsTable.getTableDescriptor(databaseConfiguration));
  }
  
  /**
   * Start initialising HBase tables by creating them. In case a table already
   * exists, the parameters configure if it must be emptied or altered in case
   * the schema is not the same.
   * 
   * @param emptyIfExists empty tables that already exist
   * @param alterIfExists alter tables that already exist and have a different
   * schema
   */
  public void start(boolean emptyIfExists, boolean alterIfExists)
      throws HBaseConnectionException {
    for (HTableDescriptor tableDesc : tables) {
      try {
        createTable(tableDesc);
        logger.info("Table `" + tableDesc.getNameAsString() + "` created.");
      } catch (TableExistsException e) {
        logger.info("Table `" + tableDesc.getNameAsString() + "` already exists.");
        
        if (emptyIfExists) {
          emptyTable(tableDesc.getName());
          logger.info("Table `" + tableDesc.getNameAsString() + "` emptied.");
        }
        
        if (alterIfExists) {
          if (modifyTable(tableDesc))
            logger.info("Table `" + tableDesc.getNameAsString() + "` modified.");
          else
            logger.info("Table `" + tableDesc.getNameAsString()
                + "` already exists and did not required modifications.");
        }
      }
    }
  }
  
  /**
   * Create an HBase table.
   * 
   * @param tableDesc
   * @throws HBaseConnectionException
   * @throws TableExistsException
   */
  public void createTable(HTableDescriptor tableDesc) 
      throws HBaseConnectionException, TableExistsException {
    Configuration conf = HBaseConfiguration.create();
    
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(conf);
      
      admin.createTable(tableDesc);
    } catch (ZooKeeperConnectionException e) {
      throw new HBaseConnectionException(e);
    } catch (MasterNotRunningException e) {
      throw new HBaseConnectionException(e);
    } catch (TableExistsException e) {
      throw e;
    } catch (IOException e) {
      throw new HBaseConnectionException(e);
    }
  }
  
  /**
   * Empties an HBase table.
   * 
   * @param tableName
   * @throws HBaseConnectionException
   */
  public void emptyTable(byte[] tableName)
      throws HBaseConnectionException {
    // TODO emptyTable
  }
  
  /**
   * Modifies schema and properties of an HBase table if they differ from those
   * provided as parameters. If they do not nothing happens. 
   * 
   * @param tableDesc
   * @return true if the table was modified
   * @throws HBaseConnectionException
   */
  public boolean modifyTable(HTableDescriptor tableDesc) 
      throws HBaseConnectionException {
    // TODO modifyTable
    
    return false;
  }
  
  // FIXME remove testing main
  public static void main(String args[]) {
    DatabaseConfiguration dbConf = new DatabaseConfiguration();
    DatabaseInitializer dbInit = new DatabaseInitializer(dbConf);
    
    try {
      dbInit.start(false, false);
    } catch (HBaseConnectionException e) {
      e.printStackTrace();
    }
  }

}
