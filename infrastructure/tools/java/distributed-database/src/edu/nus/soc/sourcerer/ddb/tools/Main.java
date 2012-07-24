package edu.nus.soc.sourcerer.ddb.tools;

import static edu.nus.soc.sourcerer.ddb.tools.DDBTools.EMPTY_EXISTING;
import static edu.nus.soc.sourcerer.ddb.tools.DDBTools.UPDATE_EXISTING;
import static edu.nus.soc.sourcerer.ddb.tools.DDBTools.HBASE_TABLE_PREFIX;

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
  
  public static final Command RETRIEVE_PROJECTS =
      new Command("retrieve-projects", "Search projects by type and ID.") {
        protected void action() {
          DDBTools.retrieveProjects();
        }
      }.setProperties(HBASE_TABLE_PREFIX);
      
  /**
   * @param args
   */
  public static void main(String[] args) {
    PropertyManager.executeCommand(args, Main.class);
  }

}
