package edu.nus.soc.sourcerer.ddb.mapreduce.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.mapreduce.map.CRMapper;
import edu.nus.soc.sourcerer.ddb.mapreduce.reduce.CRCombiner;
import edu.nus.soc.sourcerer.ddb.mapreduce.reduce.CRReducer;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

public class CRJob extends Job {

  public CRJob() throws IOException {
    super();
  }
  
  public CRJob(Configuration conf) throws IOException {
    super(conf);
    
    /*
     *  Prepare the job.
     */

    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    Scan scan = new Scan(HConstants.EMPTY_START_ROW,
        EntitiesHashHBTable.DANGLING_CACHE_START_ROW);
//    Scan scan = new Scan(Bytes.toBytes("a"), Bytes.toBytes("c"));    
//    scan.addColumn(EntitiesHashHBTable.CF_DEFAULT,
//        EntitiesHashHBTable.COL_ENTITYTYPE);
    scan.addColumn(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
    scan.addColumn(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_TARGETS_COUNT);
    scan.addColumn(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_TARGETS);
    
    setJarByClass(CRJob.class);
    TableMapReduceUtil.initTableMapperJob(
        dbConf.getTablePrefix() + EntitiesHashHBTable.NAME,
        scan, CRMapper.class, BytesWritable.class,
        DoubleWritable.class, this);
    setCombinerClass(CRCombiner.class);
    TableMapReduceUtil.initTableReducerJob(
        dbConf.getTablePrefix() + EntitiesHashHBTable.NAME,
        CRReducer.class, this);
  }

  public CRJob(Configuration conf, String jobName) throws IOException {
    this(conf);
    setJobName(jobName);
  }
}
