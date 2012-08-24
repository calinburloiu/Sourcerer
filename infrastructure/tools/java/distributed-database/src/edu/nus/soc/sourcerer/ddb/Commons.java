package edu.nus.soc.sourcerer.ddb;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.nus.soc.sourcerer.ddb.mapreduce.ConfigurationParams;

public class Commons {
  public static final String LOG_ID = "DDB";
  public static final Log LOG = LogFactory.getLog(LOG_ID);
  
  public static void setupCommonCLIArgs(Options options) {
    Option o = new Option("p", "hbase-table-prefix", true,
        "Prefix used for table names.");
    o.setArgName("prefix");
    o.setRequired(false);
    options.addOption(o);
    
    options.addOption("d", "debug", false, "Switch on DEBUG log level.");
  }
  
  public static void execCommonCLIArgs(CommandLine cmd, Configuration conf) {
    if (cmd.hasOption("d")) {
      Logger logger = Logger.getLogger(LOG_ID);
      logger.setLevel(Level.DEBUG);
      conf.set(ConfigurationParams.DEBUG, "true");
      System.out.println("DEBUG ON");
    }
    
    String tablePrefix = null;
    if (cmd.hasOption("p")) {
      tablePrefix = cmd.getOptionValue("p");
    }
    if (tablePrefix == null) {
      tablePrefix = "";
    }
    conf.set("conf.tableprefix", tablePrefix);
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(tablePrefix);
  }
}
