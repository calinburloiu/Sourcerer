package edu.nus.soc.sourcerer.drepo.tools;

import static edu.nus.soc.sourcerer.ddb.tools.DDBTools.EMPTY_EXISTING;
import static edu.nus.soc.sourcerer.ddb.tools.DDBTools.UPDATE_EXISTING;
import static edu.nus.soc.sourcerer.ddb.tools.DDBTools.HBASE_TABLE_PREFIX;
import static edu.nus.soc.sourcerer.drepo.tools.RepoTools.READ_FILES_COUNT;
import static edu.uci.ics.sourcerer.repo.general.AbstractRepository.INPUT_REPO;
import edu.uci.ics.sourcerer.util.io.Command;
import edu.uci.ics.sourcerer.util.io.PropertyManager;

public class Main {

  public static final Command INITIALIZE_REPO =
    new Command("initialize-repo", "Initialize, clean or update distributed repository.") {
      protected void action() {
        RepoTools.initializeRepo();
      }
    }.setProperties(EMPTY_EXISTING, UPDATE_EXISTING, HBASE_TABLE_PREFIX);
  
  public static final Command IMPORT_REPO =
      new Command("import-repo", "Import an old Sourcerer repo to the distributed repository.") {
        protected void action() {
          RepoTools.importRepo();
        }
      }.setProperties(INPUT_REPO, READ_FILES_COUNT, HBASE_TABLE_PREFIX);

  /**
   * @param args
   */
  public static void main(String[] args) {
    PropertyManager.executeCommand(args, Main.class);
  }

}
