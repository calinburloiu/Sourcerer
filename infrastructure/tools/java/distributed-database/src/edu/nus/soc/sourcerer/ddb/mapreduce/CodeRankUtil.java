package edu.nus.soc.sourcerer.ddb.mapreduce;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.nus.soc.sourcerer.ddb.Commons;
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.CRMetricsJob;
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.CRTopJob;
import edu.nus.soc.sourcerer.util.CLICommons;

import static edu.nus.soc.sourcerer.ddb.mapreduce.Util.REDUCE_OUTPUT_FILE;

public class CodeRankUtil
implements MapReduceApp {
  
  protected boolean crTop = false;
  protected boolean euclidianDist = false;
  protected boolean crSum = false;
  protected boolean deip = false;
  
  /** Temporary path for the output of the dangling entities inner product */
  protected static final String CRMETRICS_OUTPUT = "/tmp/crmetrics_output/";
  protected static final String CRTOP_OUTPUT = "/tmp/crtop_output/";
  
  private CommandLine processArgs(String args[], Configuration conf)
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
    
    Option opt = new Option("T", "coderank-top", false,
        "Generate a file with the top of all entities by CodeRank. "
        + "Output will be written in HDFS in '"
        + CRTOP_OUTPUT + REDUCE_OUTPUT_FILE + "'.");
    opt.setRequired(false);
    options.addOption(opt);
    
    opt = new Option("e", "metric-euclidian-dist", false,
        "Calculate euclidian distance between current CodeRank vector and the previous one. "
        + "Output will be written in HDFS in '"
        + CRMETRICS_OUTPUT + REDUCE_OUTPUT_FILE + "'.");
    opt.setRequired(false);
    options.addOption(opt);
    
    opt = new Option("s", "metric-coderanks-sum", false,
        "Calculate the sum of CodeRanks for all entities. Should be close to 1 if computation was correct. "
        + "Output will be written in HDFS in '"
        + CRMETRICS_OUTPUT + REDUCE_OUTPUT_FILE + "'.");
    opt.setRequired(false);
    options.addOption(opt);
    
    opt = new Option("D", "metric-deip", false,
        "Calculate DEIP (Dangling Entities Inner Product). "
        + "Output will be written in HDFS in '"
        + CRMETRICS_OUTPUT + REDUCE_OUTPUT_FILE + "'.");
    opt.setRequired(false);
    options.addOption(opt);
    
    // Parse CLI arguments.
    CommandLine cmd = null;
    try {
      cmd = CLICommons.parseArgs(specificArgs, options);
      Commons.execCommonCLIArgs(cmd, conf);
    } catch (ParseException e) {
      System.exit(1);
    }
    
    /*
     * Interpret CLI arguments.
     */
    // --coderank-top
    if (cmd.hasOption("T")) {
      crTop = true;
    }
    
    // --metric-euclidian-dist
    if (cmd.hasOption("e")) {
      euclidianDist = true;
    }
    
    // --metric-deip
    if (cmd.hasOption("D")) {
      deip = true;
    }
    
    // --metric-coderanks-sum
    if (cmd.hasOption("s")) {
      crSum = true;
    }
    
    return cmd;
  }

  @Override
  public void run(String[] args)
  throws HadoopException {
    Configuration conf = HBaseConfiguration.create();
    
    try {
      processArgs(args, conf);
      
      if (Util.existsPath(CRMETRICS_OUTPUT, conf)) {
        throw new InvalidCLIArgsException("Path '"
            + CRMETRICS_OUTPUT + "' already exists. Remove it and try again.");
      }
      if (Util.existsPath(CRTOP_OUTPUT, conf)) {
        throw new InvalidCLIArgsException("Path '"
            + CRTOP_OUTPUT + "' already exists. Remove it and try again.");
      }
    } catch (InvalidCLIArgsException e) {
      LOG.fatal(e.getMessage());
      System.exit(1);
    } catch (IOException e) {
      throw new HadoopException(e.getMessage(), e);
    }
    
    // Temporary HDFS output for the dangling entities inner product and others.
    if (euclidianDist || crSum || deip) {
      conf.set(ConfigurationParams.CRMETRICS_OUTPUT, CRMETRICS_OUTPUT);
      conf.set(ConfigurationParams.CRMETRICS_FILE,
          CRMETRICS_OUTPUT + REDUCE_OUTPUT_FILE);
    }
    if (crTop) {
      conf.set(ConfigurationParams.CRTOP_OUTPUT, CRTOP_OUTPUT);
      conf.set(ConfigurationParams.CRTOP_FILE,
          CRTOP_OUTPUT + REDUCE_OUTPUT_FILE);
    }
    
    conf.setBoolean(ConfigurationParams.CRMETRICS_SUM, crSum);
    conf.setBoolean(ConfigurationParams.CRMETRICS_DIST, euclidianDist);
    conf.setBoolean(ConfigurationParams.CRMETRICS_DEIP, deip);
    
    try {
      if (crTop) {
        Job topJob = new CRTopJob(conf,
            "Generate entities top by CodeRank");

        Util.runJob(topJob);
        LOG.info("Output written in HDFS in '"
            + CRTOP_OUTPUT + REDUCE_OUTPUT_FILE + "'.");
      }
      
      if (euclidianDist || crSum || deip) {
        Job metricsJob = new CRMetricsJob(conf,
            "Compute metrics by performing CodeRank vector operations");
        
        Util.runJob(metricsJob);
        LOG.info("Output written in HDFS in '"
            + CRMETRICS_OUTPUT + REDUCE_OUTPUT_FILE + "'.");
      }
    } catch (IOException e) {
      throw new HadoopException(e.getMessage(), e);
    }
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      (new CodeRankUtil()).run(args);
    } catch (HadoopException e) {
      LOG.fatal(e.getMessage(), e);
    }
  }

}
