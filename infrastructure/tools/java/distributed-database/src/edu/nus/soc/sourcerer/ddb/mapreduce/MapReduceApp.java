package edu.nus.soc.sourcerer.ddb.mapreduce;

public interface MapReduceApp {
  
  void run(String[] args) throws HadoopException;
}
