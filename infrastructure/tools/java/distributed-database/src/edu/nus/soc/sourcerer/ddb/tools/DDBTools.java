package edu.nus.soc.sourcerer.ddb.tools;

import java.sql.SQLException;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.queries.ProjectsRetriever;
import edu.nus.soc.sourcerer.model.ddb.ProjectModel;
import edu.uci.ics.sourcerer.model.Project;
import edu.uci.ics.sourcerer.util.io.Property;
import edu.uci.ics.sourcerer.util.io.properties.BooleanProperty;
import edu.uci.ics.sourcerer.util.io.properties.IntegerProperty;
import edu.uci.ics.sourcerer.util.io.properties.StringProperty;

import static edu.uci.ics.sourcerer.util.io.Logging.logger;

public class DDBTools {
  // initialize-database properties:
  public static final Property<Boolean> EMPTY_EXISTING = new BooleanProperty(
      "empty-existing",
      false,
      "Delete all data from existing tables.");
  public static final Property<Boolean> UPDATE_EXISTING = new BooleanProperty(
      "update-existing",
      false,
      "Update schema and configuration of existing tables if necessary. "
      + "This option becomes true if empty-existing is set.");
  public static final Property<String> HBASE_TABLE_PREFIX = new StringProperty(
      "hbase-table-prefix",
      "",
      "Prefix used for table names.");
  
  // import-mysql properties
  public static final Property<Integer> SELECT_ROWS_COUNT = new IntegerProperty(
      "select-rows-count",
      65536,
      "The number of rows at a time that are going to be selected from SQL in one iteration.");
  
  public static void initializeDatabase() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    try {
      DatabaseInitializer dbInit = new DatabaseInitializer();
      dbInit.start(EMPTY_EXISTING.getValue(), UPDATE_EXISTING.getValue());
    } catch (HBaseConnectionException e) {
      logger.severe("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      logger.severe("An HBase error occured: " + e.getMessage());
    }
  }
  
  public static void importMySQL() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    MySQLImporter importer = new MySQLImporter(
        dbConf, SELECT_ROWS_COUNT.getValue());
    
    try {
      importer.start();
    } catch (SQLException e) {
      logger.severe("A MySQL error occured: " + e.getMessage());
    } catch (HBaseConnectionException e) {
      logger.severe("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      logger.severe("An HBase error occured: " + e.getMessage());
    }
  }

  public static void retrieveProjects() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    ProjectsRetriever pr;
    ProjectModel project = null;
    try {
      pr = new ProjectsRetriever();
      project = pr.retrieveProjects(Project.CRAWLED, 
          new byte[] {0x06, (byte)0xCD, 0x2D, (byte)0xA6, 0x01, (byte)0x90, 0x45, 0x55, 0x24,
          0x74, (byte)0x9B, 0x0E, 0x16, (byte)0xCF, 0x50, (byte)0xDD});
    } catch (HBaseConnectionException e) {
      logger.severe("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      logger.severe("An HBase error occured: " + e.getMessage());
    }
    
    System.out.println(project);
  }
}
