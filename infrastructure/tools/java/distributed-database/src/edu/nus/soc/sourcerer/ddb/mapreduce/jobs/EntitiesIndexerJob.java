package edu.nus.soc.sourcerer.ddb.mapreduce.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.EntityWritable;
import edu.nus.soc.sourcerer.ddb.mapreduce.map.EntitiesMapper;
import edu.nus.soc.sourcerer.ddb.mapreduce.reduce.EntitiesReducer;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

public class EntitiesIndexerJob extends Job {

  public EntitiesIndexerJob() throws IOException {
    super();
  }

  public EntitiesIndexerJob(Configuration conf) throws IOException {
    super(conf);
    
    /*
     *  Prepare the job.
     */

    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    Scan scan = new Scan();
    scan.addColumn(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_ENTITYTYPE);
    scan.addColumn(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_FQN);
    scan.addColumn(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_PID);
    scan.addColumn(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_FID);
    scan.addColumn(EntitiesHashHBTable.CF_DEFAULT,
        EntitiesHashHBTable.COL_FILETYPE);
    
    setJarByClass(EntitiesIndexerJob.class);
    TableMapReduceUtil.initTableMapperJob(
        dbConf.getTablePrefix() + EntitiesHashHBTable.NAME,
        scan, EntitiesMapper.class, EntityWritable.class,
        BytesWritable.class, this);
    setReducerClass(EntitiesReducer.class);
    setOutputFormatClass(NullOutputFormat.class);
  }
  
  public EntitiesIndexerJob(Configuration conf, String jobName)
      throws IOException {
    this(conf);
    setJobName(jobName);
  }
}
