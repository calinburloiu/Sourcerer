package edu.nus.soc.sourcerer.ddb.tools;

import static edu.nus.soc.sourcerer.ddb.tools.DDBTools.*;

//import java.math.BigDecimal;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static edu.uci.ics.sourcerer.db.util.DatabaseConnection.DATABASE_PASSWORD;
import static edu.uci.ics.sourcerer.db.util.DatabaseConnection.DATABASE_URL;
import static edu.uci.ics.sourcerer.db.util.DatabaseConnection.DATABASE_USER;
import edu.uci.ics.sourcerer.util.io.Command;
import edu.uci.ics.sourcerer.util.io.PropertyManager;

public class Main {
  
  
  public static final Command INITIALIZE_DB =
      new Command("initialize-db", "Initialize, clean or update schema for the database.") {
        protected void action() {
          DDBTools.initializeDatabase();
        }
      }.setProperties(EMPTY_EXISTING, UPDATE_EXISTING, HBASE_TABLE_PREFIX);
  
  public static final Command IMPORT_MYSQL =
      new Command("import-mysql", "Import data from MySQL-based SourcererDB database.") {
        protected void action() {
          DDBTools.importMySQL();
        }
      }.setProperties(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD,
          HBASE_TABLE_PREFIX);

  public static final Command HEX_TO_ESCSTR =
      new Command("hex-to-escstr", "Convert a hex string to a binary escaped string.") {
        protected void action() {
          DDBTools.hexToEscStr();
        }
      }.setProperties(HEX_STR);
      
  public static final Command ESCSTR_TO_HEX =
      new Command("escstr-to-hex", "Convert a binary escaped string to a hex string.") {
        protected void action() {
          DDBTools.escStrToHex();
        }
      }.setProperties(ESC_STR);
  
  public static final Command RETRIEVE_PROJECTS =
      new Command("retrieve-projects", "Search projects by: pt [pid].") {
        protected void action() {
          DDBTools.retrieveProjects();
        }
      }.setProperties(PROJECT_TYPE, PROJECT_ID, HBASE_TABLE_PREFIX);
  
  public static final Command RETRIEVE_FILES =
      new Command("retrieve-files", "Search files by: pid [ft [fid]].") {
        protected void action() {
          DDBTools.retrieveFiles();
        }
      }.setProperties(PROJECT_ID, FILE_TYPE, FILE_ID, HBASE_TABLE_PREFIX);

  // TODO Other search criteria
  public static final Command RETRIEVE_ENTITIES =
      new Command("retrieve-entities", "Search entities by: eid TODO") {
        protected void action() {
          DDBTools.retrieveEntities();
        }
      }.setProperties(ENTITY_ID, FQN, FQN_PREFIX, PROJECT_ID, FILE_ID,
          FILE_TYPE, ENTITY_TYPE, HBASE_TABLE_PREFIX);
      
  public static final Command RETRIEVE_CODE_RANK =
      new Command("retrieve-code-rank", "Retrieve the code rank of an entity by its ID") {
        protected void action() {
          DDBTools.retrieveCodeRank();
        }
      }.setProperties(ENTITY_ID, HBASE_TABLE_PREFIX);

  public static final Command RETRIEVE_RELATIONS_BY_SOURCE =
      new Command("retrieve-relations-by-source", "Retrieve all relations with a particular source entity ID.") {
        protected void action() {
          DDBTools.retrieveSourcedRelations();
        }
      }.setProperties(ENTITY_ID, HBASE_TABLE_PREFIX);
      
  public static final Command RETRIEVE_RELATIONS =
      new Command("retrieve-relations", "Search relations by: rid TODO") {
        protected void action() {
          DDBTools.retrieveRelations();
        }
      }.setProperties(RELATION_ID, SOURCE_ID, TARGET_ID, RELATION_KIND,
          PROJECT_ID, FILE_ID, FILE_TYPE, HBASE_TABLE_PREFIX);
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {

//    Test.test();
//    System.exit(0);
    
    // Setup logging.
    Logger logger = Logger.getLogger("DDB");
    logger.setLevel(Level.DEBUG);
    
    PropertyManager.executeCommand(args, Main.class);
  }

}
