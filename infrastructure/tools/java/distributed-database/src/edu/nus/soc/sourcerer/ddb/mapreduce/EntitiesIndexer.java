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
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.EntitiesIndexerJob;
import edu.nus.soc.sourcerer.util.CLICommons;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

/**
 * Hadoop MapReduce application which reads data from `entities_hash` HBase
 * table and populates other tables with entity information providing
 * redundancy and indexing relations based on different fields. Output tables
 * include:
 * 
 * <ul>
 *   <li>entities</li>
 *   <li>files</li>
 * </ul>
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class EntitiesIndexer
implements MapReduceApp {

  private CommandLine processArgs(String[] args, Configuration conf)
  throws InvalidCLIArgsException {
    // Preprocess CLI arguments.
    String[] specificArgs = null;
    try {
      specificArgs = 
          new GenericOptionsParser(conf, args).getRemainingArgs();
    } catch (IOException e) {
      LOG.error(e.getMessage());
      System.exit(1);
    }
    
    // Add argument options.
    Options options = new Options();
    Commons.setupCommonCLIArgs(options);
    
    // Parse CLI arguments.
    CommandLine cmd = null;
    try {
      cmd = CLICommons.parseArgs(specificArgs, options);
      Commons.execCommonCLIArgs(cmd, conf);
    } catch (ParseException e) {
      System.exit(1);
    }
    
    return cmd;
  }
  
  @Override
  public void run(String[] args)
  throws HadoopException {
    Configuration conf = HBaseConfiguration.create();
    
    try {
      processArgs(args, conf);
    } catch (InvalidCLIArgsException e) {
      LOG.fatal(e.getMessage());
      System.exit(1);
    }

    try {
      Job job = new EntitiesIndexerJob(conf,
          "Populate entities table and relations column famility of files table with entities from entities_hash table");
      Util.runJob(job);
    } catch (IOException e) {
      throw new HadoopException(e.getMessage(), e);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      (new EntitiesIndexer()).run(args);
    } catch (HadoopException e) {
      LOG.fatal(e.getMessage(), e);
    }
  }

}
