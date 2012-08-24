package edu.nus.soc.sourcerer.ddb.mapreduce.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.mapreduce.ConfigurationParams;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.DoubleReverseComparator;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.EntitySmallWritable;
import edu.nus.soc.sourcerer.ddb.mapreduce.map.CRTopMapper;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

public class CRTopJob
extends Job {
  
  public CRTopJob() throws IOException {
    super();
  }
  
  public CRTopJob(Configuration conf) throws IOException {
    super(conf);
    
    /*
     *  Prepare the job.
     */
    
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    Scan scan = new Scan(HConstants.EMPTY_START_ROW,
        EntitiesHashHBTable.DANGLING_CACHE_START_ROW);
    scan.addColumn(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_ENTITYTYPE);
    scan.addColumn(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_FQN);
    scan.addColumn(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
    
    setJarByClass(CRTopJob.class);
    TableMapReduceUtil.initTableMapperJob(
        dbConf.getTablePrefix() + EntitiesHashHBTable.NAME,
        scan, CRTopMapper.class, DoubleWritable.class,
        EntitySmallWritable.class, this);
    setSortComparatorClass(DoubleReverseComparator.class);
    setOutputKeyClass(DoubleWritable.class);
    setOutputValueClass(EntitySmallWritable.class);
//    setNumReduceTasks(0);
    TextOutputFormat.setOutputPath(this, new Path(conf.get(
        ConfigurationParams.CRTOP_OUTPUT)));
  }

  public CRTopJob(Configuration conf, String jobName) throws IOException {
    this(conf);
    setJobName(jobName);
  }
}
