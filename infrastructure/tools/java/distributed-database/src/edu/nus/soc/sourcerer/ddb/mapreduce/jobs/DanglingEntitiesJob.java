package edu.nus.soc.sourcerer.ddb.mapreduce.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.mapreduce.ConfigurationParams;
import edu.nus.soc.sourcerer.ddb.mapreduce.map.DanglingEntitiesMapper;
import edu.nus.soc.sourcerer.ddb.mapreduce.reduce.DanglingEntitiesReducer;
import edu.nus.soc.sourcerer.ddb.tables.EntitiesHashHBTable;

//import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

@Deprecated
public class DanglingEntitiesJob extends Job {

  public DanglingEntitiesJob() throws IOException {
    super();
  }
  
  public DanglingEntitiesJob(Configuration conf) throws IOException {
    super(conf);
    
    /*
     *  Prepare the job.
     */

    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    Scan scan = new Scan(HConstants.EMPTY_START_ROW,
        EntitiesHashHBTable.DANGLING_CACHE_START_ROW);
//    Scan scan = new Scan(Bytes.toBytes("a"), Bytes.toBytes("c"));    
    scan.addColumn(EntitiesHashHBTable.CF_DEFAULT, EntitiesHashHBTable.COL_FQN);
    scan.addColumn(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_SOURCE_RANK);
    scan.addColumn(EntitiesHashHBTable.CF_RELATIONS,
        EntitiesHashHBTable.COL_RELATIONS_TARGETS_COUNT);
    
    setJarByClass(DanglingEntitiesJob.class);
    TableMapReduceUtil.initTableMapperJob(
        dbConf.getTablePrefix() + EntitiesHashHBTable.NAME,
        scan, DanglingEntitiesMapper.class, ByteWritable.class,
        DoubleWritable.class, this);
    setReducerClass(DanglingEntitiesReducer.class);
    setOutputKeyClass(ByteWritable.class);
    setOutputValueClass(DoubleWritable.class);
//    setNumReduceTasks(0);
    this.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(this, new Path(conf.get(
        ConfigurationParams.DEIP_OUTPUT)));
  }

  public DanglingEntitiesJob(Configuration conf, String jobName) throws IOException {
    this(conf);
    setJobName(jobName);
  }
}
