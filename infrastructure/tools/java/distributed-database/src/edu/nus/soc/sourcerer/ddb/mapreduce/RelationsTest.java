package edu.nus.soc.sourcerer.ddb.mapreduce;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.nus.soc.sourcerer.ddb.tables.RelationsHashHBTable;

public class RelationsTest {
  
  private static final Log LOG = LogFactory.getLog(RelationsTest.class);
  
  public static final String NAME = "RelationsTest";
  public enum Counters { ROWS, COLS, ERRORS, VALIDS };
  
  static class RelationsTestMapper extends TableMapper<Text, Text> {
    
    @Override
    public void map(ImmutableBytesWritable row, Result columns,
        Context context) throws IOException {
      context.getCounter(Counters.ROWS).increment(1);
      
      String key = Bytes.toStringBinary(row.get());
      String value = "";
      
      for (KeyValue kv : columns.list()) {
        context.getCounter(Counters.COLS).increment(1);
        value += Bytes.toStringBinary(kv.getQualifier()) + "=\""
            + Bytes.toStringBinary(kv.getValue()) + "\" ";
      }
      try {
        context.write(new Text(key), new Text(value));
        context.getCounter(Counters.VALIDS).increment(1);
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("KEY: " + Bytes.toStringBinary(row.get()) +
          ", VALUE: " + value);
        context.getCounter(Counters.ERRORS).increment(1);
      }
    }
  }
  
  /**
   * Parse the command line parameters.
   *
   * @param args The parameters to parse.
   * @return The parsed command line.
   * @throws org.apache.commons.cli.ParseException When the parsing of the parameters fails.
   */
  private static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();
    
    Option o = new Option("o", "output", true,
      "the directory to write to");
    o.setArgName("path-in-HDFS");
    o.setRequired(true);
    options.addOption(o);
    
    o = new Option("p", "hbase-table-prefix", true,
        "prefix used for table names");
    o.setArgName("prefix");
    o.setRequired(false);
    options.addOption(o);
    
    options.addOption("d", "debug", false, "switch on DEBUG log level");
    
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage() + "\n");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(NAME + " ", options, true);
      System.exit(-1);
    }
    if (cmd.hasOption("d")) {
      Logger log = Logger.getLogger("mapreduce");
      log.setLevel(Level.DEBUG);
      System.out.println("DEBUG ON");
    }
    return cmd;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] specificArgs = 
        new GenericOptionsParser(conf, args).getRemainingArgs();
    CommandLine cmd = parseArgs(specificArgs);
    String tablePrefix = "";
    
    // Arguments
    if (cmd.hasOption("d"))
      conf.set("conf.debug", "true");
    if (cmd.hasOption("p"));
      tablePrefix = cmd.getOptionValue("p");
    String output = cmd.getOptionValue("o");
    
    Scan scan = new Scan(new byte[] {'A', 'Z'}, new byte[] {'A', 'Z', 'A'});
    scan.addFamily(RelationsHashHBTable.CF_DEFAULT);
    
    Job job = new Job(conf, "Relations Test");
    job.setJarByClass(RelationsTest.class);
    TableMapReduceUtil.initTableMapperJob(
        tablePrefix + RelationsHashHBTable.NAME, scan,
        RelationsTestMapper.class, Text.class, Text.class, job);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(job, new Path(output));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
