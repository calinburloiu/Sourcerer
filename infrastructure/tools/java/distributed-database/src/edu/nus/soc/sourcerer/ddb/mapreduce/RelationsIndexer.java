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
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.CRRelationsIndexerJob;
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.RelationsIndexerJob;
import edu.nus.soc.sourcerer.util.CLICommons;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

/**
 * Hadoop MapReduce application which reads data from `relations_hash` HBase
 * table and populates other tables with relation information providing
 * redundancy and indexing relations based on different fields. Output tables
 * include:
 * 
 * <ul>
 *   <li>relations_direct</li>
 *   <li>relations_inverse</li>
 *   <li>entities_hash</li>
 *   <li>files</li>
 * </ul>
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class RelationsIndexer
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
      Job crRelJob = new CRRelationsIndexerJob(conf,
          "Populate relations column famility of entities_hash table with relations from relations_hash table");
      Util.runJob(crRelJob);
      
      Job relJob = new RelationsIndexerJob(conf,
          "Populate relations_direct table, relations_inverse table and relations column famility of files table with relations from relations_hash table");
      Util.runJob(relJob);
    } catch (IOException e) {
      throw new HadoopException(e.getMessage(), e);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      (new RelationsIndexer()).run(args);
    } catch (HadoopException e) {
      LOG.fatal(e.getMessage(), e);
    }
  }

}
