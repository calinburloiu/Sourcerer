package edu.nus.soc.sourcerer.ddb.tools;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.mapreduce.InvalidCLIArgsException;
import edu.nus.soc.sourcerer.ddb.queries.EntitiesRetriever;
import edu.nus.soc.sourcerer.ddb.queries.FilesRetriever;
import edu.nus.soc.sourcerer.ddb.queries.ProjectsRetriever;
import edu.nus.soc.sourcerer.ddb.queries.RelationsRetriever;
import edu.nus.soc.sourcerer.ddb.util.ModelAppender;
import edu.nus.soc.sourcerer.ddb.util.PrintModelAppender;
import edu.nus.soc.sourcerer.model.ddb.FileModel;
import edu.nus.soc.sourcerer.model.ddb.Model;
import edu.nus.soc.sourcerer.model.ddb.ProjectModel;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.File;
import edu.uci.ics.sourcerer.model.Project;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;
import edu.uci.ics.sourcerer.util.io.Property;
import edu.uci.ics.sourcerer.util.io.properties.BooleanProperty;
import edu.uci.ics.sourcerer.util.io.properties.IntegerProperty;
import edu.uci.ics.sourcerer.util.io.properties.StringProperty;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

public class DDBTools {
  
  public static final PrintStream printStream = System.out;//null;
  
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
      "hex",
      "Hex string.");
  
  public static final Property<String> ESC_STR = new StringProperty(
      "escstr",
      "Binary escaped string.");
  
  // retrieving properties
  public static final Property<Integer> LIMIT = new IntegerProperty(
      "limit",
      Integer.MAX_VALUE,
      "Maximum number of row to be retrieved.");
  public static final Property<String> PROJECT_TYPE = new StringProperty(
      "pt",
      "",
      "Filter results by project type expressed as a string.");
  public static final Property<String> FILE_TYPE = new StringProperty(
      "ft",
      "",
      "Filter results by file type expressed as a string.");
  public static final Property<String> ENTITY_TYPE = new StringProperty(
      "et",
      "",
      "Filter results by entity type expressed as a string.");
  public static final Property<String> RELATION_KIND = new StringProperty(
      "rk",
      "",
      "Filter results by relation kind which is a combination between "
          + "relation type and relations class expressed as two strings "
          + "separated by a double colon ':'.");
  public static final Property<String> PROJECT_ID = new StringProperty(
      "pid",
      "",
      "Filter results by project id expresed as a hex hash.");
  public static final Property<String> FILE_ID = new StringProperty(
      "fid",
      "",
      "Filter results by file id expresed as a hex hash.");
  public static final Property<String> ENTITY_ID = new StringProperty(
      "eid",
      "",
      "Filter results by entity id expresed as a hex hash.");
  public static final Property<String> RELATION_ID = new StringProperty(
      "rid",
      "",
      "Filter results by relation id expresed as a hex hash.");
  public static final Property<String> SOURCE_ID = new StringProperty(
      "seid",
      "",
      "Filter results by source entity id expresed as a hex hash.");
  public static final Property<String> TARGET_ID = new StringProperty(
      "teid",
      "",
      "Filter results by target entity id expresed as a hex hash.");
  public static final Property<String> FQN = new StringProperty(
      "fqn",
      "",
      "Filter results by the FQN (Fully Qualified Name) of an entity "
          + "expressed as a string.");
  public static final Property<String> FQN_PREFIX = new StringProperty(
      "fqn-prefix",
      "",
      "Filter results by an FQN (Fully Qualified Name) prefix of an entity "
          + "expressed as a string.");
  
  public static void initializeDatabase() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    try {
      DatabaseInitializer dbInit = new DatabaseInitializer();
      dbInit.start(EMPTY_EXISTING.getValue(), UPDATE_EXISTING.getValue());
    } catch (HBaseConnectionException e) {
      LOG.fatal("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      LOG.fatal("An HBase error occured: " + e.getMessage());
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
      LOG.fatal("A MySQL error occured: " + e.getMessage());
    } catch (HBaseConnectionException e) {
      LOG.fatal("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      LOG.fatal("An HBase error occured: " + e.getMessage());
    }
  }
  
  public static void hexToEscStr() {
    System.out.println(Bytes.toStringBinary(
        Serialization.hexStringToByteArray(HEX_STR.getValue())));
  }
  
  public static void escStrToHex() {
    System.out.println(Serialization.byteArrayToHexString(
        Serialization.escStringtoByteArray(ESC_STR.getValue())));
  }

  public static void retrieveProjects() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    ProjectsRetriever pr;
    try {
      pr = new ProjectsRetriever();
      String strType = PROJECT_TYPE.getValue();
      String projectID = PROJECT_ID.getValue();
      ModelAppender<Model> appender =
          new PrintModelAppender<Model>(true, printStream,
              LIMIT.getValue());
      if (projectID.isEmpty())
        projectID = null;
      Byte type = null;
      if (!strType.isEmpty())
        type = Project.valueOf(strType).getValue();
      
      pr.retrieveProjects(appender, type,
          Serialization.hexStringToByteArray(projectID));
    } catch (HBaseConnectionException e) {
      LOG.fatal("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      LOG.fatal("An HBase error occured: " + e.getMessage());
    } catch (IllegalArgumentException e) {
      LOG.fatal(StringUtils.stringifyException(e));
    }
  }
  
  public static void retrieveFiles() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    FilesRetriever fr = null;
    try {
      fr = new FilesRetriever();
      
      String projectID = PROJECT_ID.getValue();
      String fileType = FILE_TYPE.getValue();
      String fileID = FILE_ID.getValue();
      if (projectID.isEmpty()) projectID = null;
      if (fileType.isEmpty()) fileType = null;
      if (fileID.isEmpty()) fileID = null;
      
      ModelAppender<FileModel> appender =
          new PrintModelAppender<FileModel>(true, printStream,
              LIMIT.getValue());
      fr.retrieveFiles(appender,
          Serialization.hexStringToByteArray(projectID),
          fileType == null ? null : File.valueOf(fileType).getValue(),
          Serialization.hexStringToByteArray(fileID));
    } catch (HBaseConnectionException e) {
      LOG.fatal("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      LOG.fatal("An HBase error occured: " + e.getMessage());
    } catch (IllegalArgumentException e) {
      LOG.fatal(StringUtils.stringifyException(e));
    }
  }
  
  public static void retrieveEntities() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    EntitiesRetriever er = null;
    try {
      er = new EntitiesRetriever();
      
      String fqn = null;
      if (!FQN_PREFIX.getValue().isEmpty() && !FQN.getValue().isEmpty()) {
        throw new InvalidCLIArgsException(
            "You cannot provide both an fqn and and an fqn-prefix argument.");
      }
      else if (!FQN_PREFIX.getValue().isEmpty()) {
        fqn = FQN_PREFIX.getValue();
      }
      else if (!FQN.getValue().isEmpty()) {
        fqn = FQN.getValue() + "\0";
      }
      if (fqn != null && fqn.isEmpty()) fqn = null;
      
      String entityID = ENTITY_ID.getValue();
      if (entityID.isEmpty()) entityID = null;
      String projectID = PROJECT_ID.getValue();
      if (projectID.isEmpty()) projectID= null;
      String fileID = FILE_ID.getValue();
      if (fileID.isEmpty()) fileID = null;
      
      String fileType = FILE_TYPE.getValue();
      if (fileType.isEmpty()) fileType = null;
      Byte bFileType = null;
      try {
        bFileType = File.valueOf(fileType).getValue();
      } catch (IllegalArgumentException e) {
        throw new InvalidCLIArgsException("Unknown file type.");
      } catch (NullPointerException e) {}
      
      String entityType = ENTITY_TYPE.getValue();
      if (entityType.isEmpty()) entityType = null;
      Byte bEntityType = null;
      try {
        bEntityType = Entity.valueOf(entityType).getValue();
      } catch (IllegalArgumentException e) {
        throw new InvalidCLIArgsException("Unknown entity type.");
      } catch (NullPointerException e) {}
      
      ModelAppender<Model> appender =
          new PrintModelAppender<Model>(true, printStream,
              LIMIT.getValue());
      if (entityID != null) {
        er.retrieveEntity(appender,
            Serialization.hexStringToByteArray(entityID));
      }
      else {
        er.retrieveEntities(appender, fqn,
            Serialization.hexStringToByteArray(projectID),
            Serialization.hexStringToByteArray(fileID), bFileType, bEntityType);
      }
    } catch (HBaseConnectionException e) {
      LOG.fatal("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      LOG.fatal("An HBase error occured: " + e.getMessage());
    } catch (InvalidCLIArgsException e) {
      LOG.fatal(e.getMessage());
    }
  }
  
  public static void retrieveCodeRank() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    EntitiesRetriever er = null;
    try {
      er = new EntitiesRetriever();
      
      String entityID = ENTITY_ID.getValue();
      if (entityID.isEmpty()) entityID = null;
      
      double codeRank = er.retrieveCodeRank(
          Serialization.hexStringToByteArray(entityID));
      if (codeRank != Double.MAX_VALUE)
        System.out.println(codeRank);
      else
        System.err.println("CodeRank not available for this entity.\nDid you run the CodeRank tool?");
    } catch (HBaseConnectionException e) {
      LOG.fatal("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      LOG.fatal("An HBase error occured: " + e.getMessage());
    }
  }
  
  public static void retrieveSourcedRelations() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    RelationsRetriever rr = null;
    try {
      rr = new RelationsRetriever();
      
      String entityID = ENTITY_ID.getValue();
      if (entityID.isEmpty()) entityID = null;
      
      ModelAppender<Model> appender =
          new PrintModelAppender<Model>(true, printStream,
              LIMIT.getValue());
      rr.retrieveSourcedRelations(appender,
          Serialization.hexStringToByteArray(entityID));
    } catch (HBaseConnectionException e) {
      LOG.fatal("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      LOG.fatal("An HBase error occured: " + e.getMessage());
    }
  }
  
  public static void retrieveRelations() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    RelationsRetriever rr = null;
    try {
      rr = new RelationsRetriever();
      
      String relationID = RELATION_ID.getValue();
      if (relationID.isEmpty()) relationID = null;
      String sourceID = SOURCE_ID.getValue();
      if (sourceID.isEmpty()) sourceID = null;
      String targetID = TARGET_ID.getValue();
      if (targetID.isEmpty()) targetID = null;
      String kind = RELATION_KIND.getValue();
      if (kind.isEmpty()) kind = null;
      String projectID = PROJECT_ID.getValue();
      if (projectID.isEmpty()) projectID= null;
      String fileID = FILE_ID.getValue();
      if (fileID.isEmpty()) fileID = null;
      String fileType = FILE_TYPE.getValue();
      if (fileType.isEmpty()) fileType = null;
      Byte bFileType = null;
      try {
        bFileType = File.valueOf(fileType).getValue();
      } catch (IllegalArgumentException e) {
        throw new InvalidCLIArgsException("Unknown file type.");
      } catch (NullPointerException e) {}
      
      ModelAppender<Model> appender =
          new PrintModelAppender<Model>(true, printStream, LIMIT.getValue());
      if (relationID != null) {
        rr.retrieveRelations(appender,
            Serialization.hexStringToByteArray(relationID));
      }
      else {
        rr.retrieveRelations(appender, 
            Serialization.hexStringToByteArray(sourceID),
            convKindFromStringToByte(kind),
            Serialization.hexStringToByteArray(targetID),
            Serialization.hexStringToByteArray(projectID),
            Serialization.hexStringToByteArray(fileID), bFileType);
//        rr.retrieveRelationsFromFiles(appender,
//            Serialization.hexStringToByteArray(projectID),
//            bFileType, Serialization.hexStringToByteArray(fileID),
//            convKindFromStringToByte(kind),
//            Serialization.hexStringToByteArray(targetID),
//            Serialization.hexStringToByteArray(sourceID));
      }
    } catch (HBaseConnectionException e) {
      LOG.fatal("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      LOG.fatal("An HBase error occured: " + e.getMessage());
    } catch (InvalidCLIArgsException e) {
      LOG.fatal(e.getMessage());
    }
  }
  
  public static Byte convKindFromStringToByte(String kind)
  throws InvalidCLIArgsException {
    if (kind == null || kind.isEmpty())
      return null;
    
    StringTokenizer tok = new StringTokenizer(kind, ":");
    String type = null, class_ = null;
    try {
      type = tok.nextToken();
      class_ = tok.nextToken();
    } catch (NoSuchElementException e) {
      throw new InvalidCLIArgsException(
          "Relation kind needs to be expressed as relation type and relation class separated by a double colon.");
    }

    Byte ret = null;
    try {
      ret = (byte) (Relation.valueOf(type).getValue()
          | RelationClass.valueOf(class_).getValue());
    } catch (IllegalArgumentException e) {
      throw new InvalidCLIArgsException("Unknown relation kind.");
    }
    return ret;
  }
}
