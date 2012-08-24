package edu.nus.soc.sourcerer.ddb.mapreduce;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.StringUtils;

import edu.nus.soc.sourcerer.ddb.mapreduce.jobs.CRMetricsJob;

public class Util {
  
  protected static final String REDUCE_OUTPUT_FILE = "part-r-00000";
  protected static final String MAP_OUTPUT_FILE = "part-m-00000";

  public static void printTaskError(Exception e, TaskAttemptID taid,
      Writable key) {
    System.err.println("An error occured in task attempt ID "
        + taid + " while processing input key " + key + ": "
        + StringUtils.stringifyException(e));
  }

  public static void runJob(Job job)
  throws HadoopException {
    LOG.info("Running job \"" + job.getJobName() + "\"");
    boolean success;
    try {
      success = job.waitForCompletion(true);
    } catch (Exception e) {
      throw new HadoopException("Error while waiting for job \""
          + job.getJobName() + "\": " + e.getMessage(), e);
    }
    
    if (!success)
      throw new HadoopException("\"" + job.getJobName() + "\" failed.");
  }
  
  public static boolean existsPath(String strPath, Configuration conf)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(strPath);
    
    return fs.exists(path);
  }
  
  public static void deleteFile(String strPath, Configuration conf)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(strPath);
    if (fs.exists(path))
      fs.delete(path, true);
  }
  
  public static void moveFile(String src, String dest, Configuration conf)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path srcPath = new Path(src);
    Path destPath = new Path(dest);
    
    fs.rename(srcPath, destPath);
  }
  
  public static void makeDir(String dir, Configuration conf)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(dir);
    
    fs.mkdirs(dirPath);
  }
  
  public static double readDEIPFromDEIPFile(String deipFile,
      Configuration conf)
  throws IOException {
    Path deipPath = new Path(deipFile);
    FileSystem fs = FileSystem.get(conf);
    double deip = 0.0d;
    if (!fs.exists(deipPath) || !fs.isFile(deipPath))
      throw new RuntimeException("DEIP file does not exist!");
    
    // Read DEIP (Dangling Entities Inner Product).
    SequenceFile.Reader seqReader = null;
    try {
      seqReader = new SequenceFile.Reader(fs, deipPath, conf);
      ByteWritable key = new ByteWritable();
      DoubleWritable value = new DoubleWritable();
      
      seqReader.next(key, value);
      deip = value.get();
    } finally {
      IOUtils.closeStream(seqReader);
    }
    
    return deip;
  }
  
  public static double readMetricFromFile(String metricsFile,
      CRMetricsJob.Calcs metric, Configuration conf)
  throws IOException {
    Path deipPath = new Path(metricsFile);
    FileSystem fs = FileSystem.get(conf);
    double deip = 0.0d;
    if (!fs.exists(deipPath) || !fs.isFile(deipPath))
      throw new RuntimeException("DEIP file '" + metricsFile
          + "' does not exist!");
    
    // Read DEIP (Dangling Entities Inner Product).
    SequenceFile.Reader seqReader = null;
    try {
      seqReader = new SequenceFile.Reader(fs, deipPath, conf);
      Text key = new Text();
      DoubleWritable value = new DoubleWritable();
      
      while (seqReader.next(key, value)) {
        if (Bytes.compareTo(Bytes.head(key.getBytes(), key.getLength()), 
            Bytes.toBytes(metric.getValue())) == 0) {
          deip = value.get();
          break;
        }
      }
    } finally {
      IOUtils.closeStream(seqReader);
    }
    
    return deip;
  }
}
