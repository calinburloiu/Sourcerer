package edu.nus.soc.sourcerer.ddb.mapreduce.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.mapreduce.map.CRInitMapper;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

public class CRInitJob extends Job {

  public CRInitJob() throws IOException {
    super();
  }
  
  public CRInitJob(Configuration conf) throws IOException {
    super(conf);
    
    /*
     *  Prepare the job.
     */

    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    Scan scan = new Scan(HConstants.EMPTY_START_ROW,
        EntitiesHashHBTable.DANGLING_CACHE_START_ROW);
//    Scan scan = new Scan(Bytes.toBytes("a"), Bytes.toBytes("c"));    
    scan.addFamily(EntitiesHashHBTable.CF_DEFAULT);
    scan.addFamily(EntitiesHashHBTable.CF_RELATIONS);
    
    setJarByClass(CRInitJob.class);
    TableMapReduceUtil.initTableMapperJob(
        dbConf.getTablePrefix() + EntitiesHashHBTable.NAME,
        scan, CRInitMapper.class, ImmutableBytesWritable.class,
        Writable.class, this);
    TableMapReduceUtil.initTableReducerJob(
        dbConf.getTablePrefix() + EntitiesHashHBTable.NAME,
        IdentityTableReducer.class, this);
    setNumReduceTasks(0);
  }

  public CRInitJob(Configuration conf, String jobName) throws IOException {
    this(conf);
    setJobName(jobName);
  }
}
