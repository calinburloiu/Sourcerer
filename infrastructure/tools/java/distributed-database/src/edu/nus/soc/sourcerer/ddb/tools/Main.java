package edu.nus.soc.sourcerer.ddb.tools;

import static edu.nus.soc.sourcerer.ddb.tools.DDBTools.*;

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

  public static final Command HEX_TO_HBASESTR =
      new Command("hex-to-hbasestr", "Convert a hex string to an HBase IRB string.") {
        protected void action() {
          DDBTools.hexToHBaseStr();
        }
      }.setProperties(HEX_STR);
      
//  public static final Command HBASESTR_TO_HEX =
//      new Command("hbasestr-to-hex", "Convert an HBase IRB string to a hex string.") {
//        protected void action() {
//          DDBTools.hBaseStrToHex();
//        }
//      }.setProperties(HBASE_STR);
  
  public static final Command RETRIEVE_PROJECTS =
      new Command("retrieve-projects", "Search projects by: project-type [project-id].") {
        protected void action() {
          DDBTools.retrieveProjects();
        }
      }.setProperties(PROJECT_TYPE, PROJECT_ID, HBASE_TABLE_PREFIX);
  
  public static final Command RETRIEVE_FILES =
      new Command("retrieve-files", "Search files by: project-id [file-type [file-id]].") {
        protected void action() {
          DDBTools.retrieveFiles();
        }
      }.setProperties(PROJECT_ID, FILE_TYPE, FILE_ID, HBASE_TABLE_PREFIX);
      
  /**
   * @param args
   */
  public static void main(String[] args) {
    PropertyManager.executeCommand(args, Main.class);
  }

}
