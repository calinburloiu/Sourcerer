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
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.CRInitJob;
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.CRJob;
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.CRMetricsJob;
import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.DEIPJob;
import edu.nus.soc.sourcerer.util.CLICommons;

import static edu.nus.soc.sourcerer.ddb.mapreduce.Util.REDUCE_OUTPUT_FILE;
//import static edu.nus.soc.sourcerer.ddb.mapreduce.Util.MAP_OUTPUT_FILE;

public class CodeRankCalculator {
  
  protected int nIter = 0;
  protected boolean init = false;
  protected boolean euclidianDist = false;
  protected boolean crSum = false;
  protected double tolerance = 0.0d;
  protected String metricsOutput = null;
  
  /** Temporary path for the output of the dangling entities inner product */
  protected static final String DEIP_OUTPUT = "/tmp/deip_output/";
  protected static final String CRMETRICS_OUTPUT = "/tmp/crmetrics_output/";
  
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
    
    Option opt = new Option("n", "num-iter", true,
        "The number of CodeRank iterations to run.");
    opt.setArgName("iterations_count");
    opt.setRequired(true);
    options.addOption(opt);
    
    opt = new Option("i", "init", false,
        "Initialize database before CodeRank calculation. Required if tables has just been populated.");
    opt.setRequired(false);
    options.addOption(opt);
    
    opt = new Option("c", "entities-count", true,
        "The number of entities.");
    opt.setArgName("count");
    opt.setRequired(true);
    options.addOption(opt);
    
    opt = new Option("r", "teleportation-probab", true,
        "Probability of jumping from one entity to another random one.");
    opt.setArgName("probability");
    opt.setRequired(false);
    options.addOption(opt);
    
    opt = new Option("e", "metric-euclidian-dist", false,
        "Calculate euclidian distance between current CodeRank vector and the previous one. "
        + "Use --tolerance / -t argument to set a distance when computation should stop.");
    opt.setRequired(false);
    options.addOption(opt);
    
    opt = new Option("t", "tolerance", true,
        "Euclidian distance between currenct iteration and the previous one which stops computation if it is reached. "
        + "Requires setting --metric-euclidian-dist / -e argument.");
    opt.setArgName("tolerance");
    opt.setRequired(false);
    options.addOption(opt);
    
    opt = new Option("s", "metric-coderanks-sum", false,
        "Calculate the sum of CodeRanks for all entities. Should be close to 1 if computation was correct.");
    opt.setRequired(false);
    options.addOption(opt);
    
    opt = new Option("o", "metrics-output", true,
        "Output directory in HDFS where metrics should be saved (one file for each iteration). "
        + "This argument is ignored if calculation of no metric is requested. One of the arguments "
        + "--metric-euclidian-dist / -e or --metric-coderanks-sum / -s should be set. "
        + "Output file(s) will contain by default an additional metric which is \"deip\" "
        + "(dangling entities inner product).");
    opt.setArgName("HDFS-dir");
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
    // --num-iter
    if (cmd.hasOption("n")) {
      nIter = Integer.parseInt(cmd.getOptionValue("n"));
    }
    
    // --init
    if (cmd.hasOption("i")) {
      init = true;
      if (!cmd.hasOption("c")) {
        throw new InvalidCLIArgsException(
            "Argument --entities-count / -c is required if --init / -i is set.");
      }
    }
    
    // --entities-count
    if (cmd.hasOption("c")) {
      long entitiesCount = Long.MAX_VALUE;
      entitiesCount = Long.parseLong(cmd.getOptionValue("c"));
      if (entitiesCount <= 0)
        throw new InvalidCLIArgsException(
            "Argument --entities-count / -c must provide a positive integer.");
      conf.setLong(ConfigurationParams.ENTITIES_COUNT, entitiesCount);
    }
    
    // --teleportation-probab
    if (cmd.hasOption("r")) {
      conf.setFloat(ConfigurationParams.CR_TELEPORTATION, 0.15f);
    }
    
    // --metric-euclidian-dist
    if (cmd.hasOption("e")) {
      euclidianDist = true;
    }
    
    // --tolerance
    if (cmd.hasOption("t")) {
      if (!cmd.hasOption("e")) {
        throw new InvalidCLIArgsException(
            "Argument --metric-euclidian-dist / -e is required if --tolerance / -t is set.");
      }
      
      tolerance = Double.parseDouble(cmd.getOptionValue("t"));
    }
    
    // --metric-coderanks-sum
    if (cmd.hasOption("s")) {
      crSum = true;
    }
    
    // --metrics-output
    if (cmd.hasOption("o")) {
      if (euclidianDist || crSum) {
        metricsOutput = cmd.getOptionValue("o");
      }
    }
    
    return cmd;
  }
  
  public void run(String[] args)
  throws HadoopException {
    Configuration conf = HBaseConfiguration.create();
    
    try {
      processArgs(args, conf);
    } catch (InvalidCLIArgsException e) {
      LOG.fatal(e.getMessage());
      System.exit(1);
    }
    
    // Temporary HDFS output for the dangling entities inner product and others.
    if (euclidianDist || crSum) {
      conf.set(ConfigurationParams.CRMETRICS_OUTPUT, CRMETRICS_OUTPUT);
      conf.set(ConfigurationParams.CRMETRICS_FILE,
          CRMETRICS_OUTPUT + REDUCE_OUTPUT_FILE);
    }
    else {
      conf.set(ConfigurationParams.DEIP_OUTPUT, DEIP_OUTPUT);
      conf.set(ConfigurationParams.DEIP_FILE,
          DEIP_OUTPUT + REDUCE_OUTPUT_FILE);
    }
    
    conf.setBoolean(ConfigurationParams.CRMETRICS_SUM, crSum);
    conf.setBoolean(ConfigurationParams.CRMETRICS_DIST, euclidianDist);
    if (crSum || euclidianDist) {
      conf.setBoolean(ConfigurationParams.CRMETRICS_DEIP, true);
    }
    
    // Prepare file system for metrics output: delete/recreate output directory
    if (metricsOutput != null 
        && conf.get(ConfigurationParams.CRMETRICS_FILE) != null) {
      try {
        Util.deleteFile(metricsOutput, conf);
        Util.makeDir(metricsOutput, conf);
      } catch (IOException e) {
        throw new HadoopException(e.getMessage(), e);
      }
    }
    
    LOG.debug(ConfigurationParams.CRMETRICS_OUTPUT+": "+conf.get(ConfigurationParams.CRMETRICS_OUTPUT));
    LOG.debug(ConfigurationParams.CRMETRICS_FILE+": "+conf.get(ConfigurationParams.CRMETRICS_FILE));
    LOG.debug(ConfigurationParams.DEIP_OUTPUT+": "+conf.get(ConfigurationParams.DEIP_OUTPUT));
    LOG.debug(ConfigurationParams.DEIP_FILE+": "+conf.get(ConfigurationParams.DEIP_FILE));
    LOG.debug(ConfigurationParams.CRMETRICS_SUM+": "+conf.getBoolean(ConfigurationParams.CRMETRICS_SUM, false));
    LOG.debug(ConfigurationParams.CRMETRICS_DIST+": "+conf.getBoolean(ConfigurationParams.CRMETRICS_DIST, false));
    LOG.debug(ConfigurationParams.CRMETRICS_DEIP+": "+conf.getBoolean(ConfigurationParams.CRMETRICS_DEIP, false));
    LOG.debug("tolerance: " + tolerance);
    
    Job initJob = null, deipJob = null, crMetricsJob = null, crJob = null;
    
    try {
      if (init) {
        initJob = new CRInitJob(conf,
            "Initialize database for CodeRank calculation");

        Util.runJob(initJob);
      }
      
      double euclidianDistVal = Double.MAX_VALUE;
      for (int i = 0; i < nIter; i++) {
        LOG.info("Starting CodeRank iteration " + i);
        
        // Calculate DEIP and maybe other metrics.
        if (euclidianDist || crSum) {
          // During first iteration after init no euclidian distance can be
          // calculated.
          if (init && i == 0) {
            conf.setBoolean(ConfigurationParams.CRMETRICS_DIST, false);
          }
          
          // Initialize job for DEIP and other metrics.
          crMetricsJob = new CRMetricsJob(conf,
              "Compute metrics by performing CodeRank vector operations");
          
          // Cleanup temporary files from previous iterations.
          Util.deleteFile(CRMETRICS_OUTPUT, conf);
          
          // Run job for DEIP and other metrics.
          Util.runJob(crMetricsJob);
          
          // After first iteration euclidian distance can be calculated.
          if (init && i == 0) {
            conf.setBoolean(ConfigurationParams.CRMETRICS_DIST, euclidianDist);
          }
          
          // Check if tolerance was reached.
          if (i > 0) {
            euclidianDistVal = Util.readMetricFromFile(
                conf.get(ConfigurationParams.CRMETRICS_FILE),
                CRMetricsJob.Calcs.DIST, conf);
            LOG.debug("euclidianDistance: " + euclidianDistVal);
            if (euclidianDistVal < tolerance) {
              LOG.info("CodeRank computation stopped at iteration " + i
                  + " because tolerance was reached.");
              break;
            }
          }
        }
        else {
          // Initialize job for DEIP.
          deipJob = new DEIPJob(conf,
              "Calculate dangling entities inner product");
          
          // Cleanup temporary files from previous iterations.
          Util.deleteFile(DEIP_OUTPUT, conf);
          
          // Run job for DEIP.
          Util.runJob(deipJob);
        }
        
        // Initialize job for CodeRank iteration.
        crJob = new CRJob(conf,
            "Compute CodeRank iteration");
      
        // Run job for CodeRank iteration.
        Util.runJob(crJob);
        
        if (metricsOutput != null 
            && conf.get(ConfigurationParams.CRMETRICS_FILE) != null) {
          String fileName = String.format("%03d", i);
          Util.moveFile(conf.get(ConfigurationParams.CRMETRICS_FILE),
              metricsOutput
              + (metricsOutput.endsWith("/") ? "" : "/") + fileName, conf);
        }
      }
    } catch (IOException e) {
      throw new HadoopException(e.getMessage(), e);
    }
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    
//    System.exit(0);
    
    try {
      (new CodeRankCalculator()).run(args);
    } catch (HadoopException e) {
      LOG.fatal(e.getMessage(), e);
    }
  }

}
