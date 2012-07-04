package edu.nus.soc.sourcerer.drepo.tools;

import static edu.uci.ics.sourcerer.util.io.Logging.logger;
import static edu.nus.soc.sourcerer.ddb.tools.DDBTools.HBASE_TABLE_PREFIX;
import static edu.nus.soc.sourcerer.ddb.tools.DDBTools.EMPTY_EXISTING;
import static edu.nus.soc.sourcerer.ddb.tools.DDBTools.UPDATE_EXISTING;
import static edu.uci.ics.sourcerer.repo.general.AbstractRepository.INPUT_REPO;

import java.io.File;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.SourcesHBTable;
import edu.nus.soc.sourcerer.ddb.tools.DatabaseInitializer;
import edu.uci.ics.sourcerer.util.io.Property;
import edu.uci.ics.sourcerer.util.io.properties.FileProperty;
import edu.uci.ics.sourcerer.util.io.properties.IntegerProperty;

public class RepoTools {
  
  public static final Property<Integer> READ_FILES_COUNT = new IntegerProperty(
      "read-files-count",
      64,
      "The number of files at a time that are going to be read and added to the repo in one iteration.");
  
  public static void initializeRepo() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    try {
      DatabaseInitializer dbInit =
          new DatabaseInitializer(SourcesHBTable.getTableDescriptor());
      dbInit.start(EMPTY_EXISTING.getValue(),
          UPDATE_EXISTING.getValue());
    } catch (HBaseConnectionException e) {
      logger.severe("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      logger.severe("An HBase error occured: " + e.getMessage());
    }
  }
  
  public static void importRepo() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(HBASE_TABLE_PREFIX.getValue());
    
    RepoImporter importer = new RepoImporter(INPUT_REPO.getValue(), dbConf,
        READ_FILES_COUNT.getValue());
    try {
      importer.start();
    } catch (HBaseConnectionException e) {
      logger.severe("Could not connect to HBase database: " + e.getMessage());
    } catch (HBaseException e) {
      logger.severe("An HBase error occured: " + e.getMessage());
    }
  }
}
