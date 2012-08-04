package edu.nus.soc.sourcerer.ddb.mapreduce;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.nus.soc.sourcerer.ddb.Commons;
import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.RelationsHashToEntitiesHashJob;
import edu.nus.soc.sourcerer.util.CLICommons;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

public class RelationsHashToEntitiesHash {

  
  /**
   * @param args
   */
  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    
    // Parse CLI arguments.
    String[] specificArgs = null;
    try {
      specificArgs = 
          new GenericOptionsParser(conf, args).getRemainingArgs();
    } catch (IOException e) {
      LOG.error(e.getMessage());
      System.exit(1);
    }
    Options options = new Options();
    Commons.setupCommonCLIArgs(options);
    CommandLine cmd = null;
    try {
      cmd = CLICommons.parseArgs(specificArgs, options);
      Commons.execCommonCLIArgs(cmd);
    } catch (ParseException e) {
      System.exit(1);
    }
    
    // Interpret CLI arguments.
    if (cmd.hasOption("d"))
      conf.set("conf.debug", "true");
    
    String tablePrefix = null;
    if (cmd.hasOption("p"));
      tablePrefix = cmd.getOptionValue("p");
    if (tablePrefix == null)
      tablePrefix = "";
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix(tablePrefix);
    
    try {
      Job job = new RelationsHashToEntitiesHashJob(conf, "Populate relations column famility of entities_hash table with relations from relations_hash table");
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } catch (Exception e) {
      LOG.error("Could not run MapReduce job.");
      System.exit(1);
    }
  }

}
