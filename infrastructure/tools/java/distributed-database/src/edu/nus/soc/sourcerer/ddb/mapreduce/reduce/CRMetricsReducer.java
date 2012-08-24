package edu.nus.soc.sourcerer.ddb.mapreduce.reduce;

public class CRMetricsReducer
extends CRMetricsCombiner {

  @Override
  protected double finishOperation(double d) {
    return Math.sqrt(d);
  };

}
