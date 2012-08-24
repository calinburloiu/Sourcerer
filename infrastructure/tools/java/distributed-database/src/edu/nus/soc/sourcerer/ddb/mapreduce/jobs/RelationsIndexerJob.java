package edu.nus.soc.sourcerer.ddb.mapreduce.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.mapreduce.io.RelationWritable;
import edu.nus.soc.sourcerer.ddb.mapreduce.map.RelationsMapper;
import edu.nus.soc.sourcerer.ddb.mapreduce.reduce.RelationsReducer;
import edu.nus.soc.sourcerer.ddb.tables.RelationsHashHBTable;

public class RelationsIndexerJob extends Job {

  public RelationsIndexerJob() throws IOException {
    super();
  }

  public RelationsIndexerJob(Configuration conf) throws IOException {
    super(conf);
    
    /*
     *  Prepare the job.
     */

    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    Scan scan = new Scan();
    scan.addColumn(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_KIND);
    scan.addColumn(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_SOURCE_ID);
    scan.addColumn(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_TARGET_ID);
    scan.addColumn(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_PROJECT_ID);
    scan.addColumn(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_FILE_ID);
    scan.addColumn(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_FILE_TYPE);
    scan.addColumn(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_OFFSET);
    scan.addColumn(RelationsHashHBTable.CF_DEFAULT,
        RelationsHashHBTable.COL_LENGTH);
    
    setJarByClass(RelationsIndexerJob.class);
    TableMapReduceUtil.initTableMapperJob(
        dbConf.getTablePrefix() + RelationsHashHBTable.NAME,
        scan, RelationsMapper.class, RelationWritable.class,
        BytesWritable.class, this);
    setReducerClass(RelationsReducer.class);
    setOutputFormatClass(NullOutputFormat.class);
  }
  
  public RelationsIndexerJob(Configuration conf, String jobName)
      throws IOException {
    this(conf);
    setJobName(jobName);
  }
}
