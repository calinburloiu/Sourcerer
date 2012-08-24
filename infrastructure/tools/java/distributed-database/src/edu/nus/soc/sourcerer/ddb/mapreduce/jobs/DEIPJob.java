package edu.nus.soc.sourcerer.ddb.mapreduce.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.mapreduce.ConfigurationParams;
import edu.nus.soc.sourcerer.ddb.mapreduce.map.DEIPMapper;
import edu.nus.soc.sourcerer.ddb.mapreduce.reduce.DEIPReducer;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

public class DEIPJob
extends Job {
  
  public DEIPJob() throws IOException {
    super();
  }
  
  public DEIPJob(Configuration conf) throws IOException {
    super(conf);
    
    /*
     *  Prepare the job.
     */

    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    Scan scan = new Scan(EntitiesHashHBTable.DANGLING_CACHE_START_ROW);
//    Scan scan = new Scan(Bytes.toBytes("a"), Bytes.toBytes("c"));    
    scan.addColumn(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
    
    setJarByClass(DEIPJob.class);
    TableMapReduceUtil.initTableMapperJob(
        dbConf.getTablePrefix() + EntitiesHashHBTable.NAME,
        scan, DEIPMapper.class, ByteWritable.class,
        DoubleWritable.class, this);
    setCombinerClass(DEIPReducer.class);
    setReducerClass(DEIPReducer.class);
    setOutputKeyClass(ByteWritable.class);
    setOutputValueClass(DoubleWritable.class);
//    setNumReduceTasks(0);
    setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(this, new Path(conf.get(
        ConfigurationParams.DEIP_OUTPUT)));
  }

  public DEIPJob(Configuration conf, String jobName) throws IOException {
    this(conf);
    setJobName(jobName);
  }
}
