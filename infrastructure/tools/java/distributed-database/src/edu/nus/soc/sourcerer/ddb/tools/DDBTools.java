package edu.nus.soc.sourcerer.ddb.tools;

import java.sql.SQLException;

import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.queries.FilesRetriever;
import edu.nus.soc.sourcerer.ddb.queries.ProjectsRetriever;
import edu.nus.soc.sourcerer.ddb.util.ListModelAppender;
import edu.nus.soc.sourcerer.model.ddb.FileModel;
import edu.nus.soc.sourcerer.model.ddb.ProjectModel;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.File;
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
  
  public static final Property<String> HEX_STR = new StringProperty(
      "hex-str",
      "Hex string.");
  
  public static final Property<String> HBASE_STR = new StringProperty(
      "hbase-str",
      "HBase IRB string.");
  
  // retrieving properties
  public static final Property<String> PROJECT_TYPE = new StringProperty(
      "project-type",
      "",
      "Filter results by project type expressed as a string.");
  public static final Property<String> FILE_TYPE = new StringProperty(
      "file-type",
      "",
      "Filter results by file type expressed as a string.");
  public static final Property<String> PROJECT_ID = new StringProperty(
      "project-id",
      "",
      "Filter results by project id expresed as a hex hash.");
  public static final Property<String> FILE_ID = new StringProperty(
      "file-id",
      "",
      "Filter results by file id expresed as a hex hash.");
  
  
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
  
  public static void hexToHBaseStr() {
    logger.info(Bytes.toStringBinary(
        Serialization.hexStringToByteArray(HEX_STR.getValue())));
  }
  
//  public static void hBaseStrToHex() {
//    logger.info(Serialization.byteArrayToHexString(
//        Bytes.toBytesBinary(HBASE_STR.getValue())));
//  }

  public static void retrieveProjects() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    ProjectsRetriever pr;
    try {
      pr = new ProjectsRetriever();
      String projectID = PROJECT_ID.getValue();
      ListModelAppender<ProjectModel> appender =
          new ListModelAppender<ProjectModel>();
      if (projectID.isEmpty())
        projectID = null;
      
      pr.retrieveProjects(appender, Project.valueOf(PROJECT_TYPE.getValue()).getValue(),
          Serialization.hexStringToByteArray(projectID));
      
      for (ProjectModel project : appender.getList())
        System.out.println("* " + project);
    } catch (HBaseConnectionException e) {
      logger.severe("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      logger.severe("An HBase error occured: " + e.getMessage());
    } catch (IllegalArgumentException e) {
      logger.severe("Illegal arguments: " + e.getMessage());
    }
  }
  
  public static void retrieveFiles() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    FilesRetriever fr;
    try {
      fr = new FilesRetriever();
      
      String projectID = PROJECT_ID.getValue();
      String fileType = FILE_TYPE.getValue();
      String fileID = FILE_ID.getValue();
      if (projectID.isEmpty()) projectID = null;
      if (fileType.isEmpty()) fileType = null;
      if (fileID.isEmpty()) fileID = null;
      
      ListModelAppender<FileModel> appender =
          new ListModelAppender<FileModel>();
      fr.retrieveFiles(appender,
          Serialization.hexStringToByteArray(projectID),
          fileType == null ? null : File.valueOf(fileType).getValue(),
          Serialization.hexStringToByteArray(fileID));
      
      for (FileModel file : appender.getList())
        System.out.println("* " + file);
    } catch (HBaseConnectionException e) {
      logger.severe("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      logger.severe("An HBase error occured: " + e.getMessage());
    } catch (IllegalArgumentException e) {
      logger.severe("Illegal arguments: " + e.getMessage());
    }
  }
}
