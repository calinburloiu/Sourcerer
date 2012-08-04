package edu.nus.soc.sourcerer.ddb.mapreduce.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationSourceWritable;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationTargetsWritable;
import edu.nus.soc.sourcerer.ddb.mapreduce.map.RelationsSourceMapper;
import edu.nus.soc.sourcerer.ddb.mapreduce.reduce.RelationsSourceReducer;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;
import edu.nus.soc.sourcerer.ddb.tables.RelationsHashHBTable;

public class RelationsHashToEntitiesHashJob extends Job {

  public RelationsHashToEntitiesHashJob() throws IOException {
    super();
  }

  public RelationsHashToEntitiesHashJob(Configuration conf) throws IOException {
    super(conf);
    
    /*
     *  Prepare the job.
     */
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    Scan scan = new Scan();
    
    setJarByClass(RelationsHashToEntitiesHashJob.class);
    TableMapReduceUtil.initTableMapperJob(
        dbConf.getTablePrefix() + RelationsHashHBTable.NAME,
        scan, RelationsSourceMapper.class, RelationSourceWritable.class,
        RelationTargetsWritable.class, this);
    TableMapReduceUtil.initTableReducerJob(
        dbConf.getTablePrefix() + EntitiesHashHBTable.NAME,
        RelationsSourceReducer.class, this);
  }
  
  public RelationsHashToEntitiesHashJob(Configuration conf, String jobName)
      throws IOException {
    this(conf);
    setJobName(jobName);
  }
}
