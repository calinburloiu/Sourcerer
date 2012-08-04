package edu.nus.soc.sourcerer.ddb;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Commons {
  public static final String LOG_ID = "DDB";
  public static final Log LOG = LogFactory.getLog(LOG_ID);
  
  public static void setupCommonCLIArgs(Options options) {
    Option o = new Option("p", "hbase-table-prefix", true,
        "prefix used for table names");
    o.setArgName("prefix");
    o.setRequired(false);
    options.addOption(o);
    
    options.addOption("d", "debug", false, "switch on DEBUG log level");
  }
  
  public static void execCommonCLIArgs(CommandLine cmd) {
    if (cmd.hasOption("d")) {
      Logger logger = Logger.getLogger(LOG_ID);
      logger.setLevel(Level.DEBUG);
      System.out.println("DEBUG ON");
    }
  }
}
