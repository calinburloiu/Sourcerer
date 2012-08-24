package edu.nus.soc.sourcerer.ddb.mapreduce.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.mapreduce.ConfigurationParams;
import edu.nus.soc.sourcerer.ddb.mapreduce.map.CRMetricsMapper;
import edu.nus.soc.sourcerer.ddb.mapreduce.reduce.CRMetricsCombiner;
import edu.nus.soc.sourcerer.ddb.mapreduce.reduce.CRMetricsReducer;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

public class CRMetricsJob extends Job {
  
  public enum Calcs {
    SUM("sum"),
    DIST("dist"),
    DEIP("deip");
    
    private String value;
    private Calcs(String value) { this.value = value; }
    public String getValue() { return value; }
  };
  
  public CRMetricsJob() throws IOException {
    super();
  }
  
  public CRMetricsJob(Configuration conf) throws IOException {
    super(conf);
    
    /*
     *  Prepare the job.
     */

    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    Scan scan = new Scan(HConstants.EMPTY_START_ROW,
        EntitiesHashHBTable.DANGLING_CACHE_START_ROW);
//    Scan scan = new Scan(Bytes.toBytes("a"), Bytes.toBytes("c"));    
    scan.addColumn(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
    scan.addColumn(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_TARGETS_COUNT);
    scan.setMaxVersions(2);
    
    setJarByClass(CRMetricsJob.class);
    TableMapReduceUtil.initTableMapperJob(
        dbConf.getTablePrefix() + EntitiesHashHBTable.NAME,
        scan, CRMetricsMapper.class, Text.class,
        DoubleWritable.class, this);
    setCombinerClass(CRMetricsCombiner.class);
    setReducerClass(CRMetricsReducer.class); 
    setOutputKeyClass(Text.class);
    setOutputValueClass(DoubleWritable.class);
//    setNumReduceTasks(0);
    this.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(this, new Path(conf.get(
        ConfigurationParams.CRMETRICS_OUTPUT)));
  }

  public CRMetricsJob(Configuration conf, String jobName) throws IOException {
    this(conf);
    setJobName(jobName);
  }
}
