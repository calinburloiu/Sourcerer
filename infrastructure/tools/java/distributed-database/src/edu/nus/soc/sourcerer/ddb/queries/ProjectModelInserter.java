package edu.nus.soc.sourcerer.ddb.queries;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.ProjectsHBTable;
import edu.nus.soc.sourcerer.model.ddb.ProjectModel;
import edu.nus.soc.sourcerer.util.StringSerializationException;

public class ProjectModelInserter implements ModelInserter<ProjectModel> {
  protected int selectRowsCount;
  protected HTable table;
  
  public ProjectModelInserter(int selectRowsCount)
      throws HBaseConnectionException {
    super();
    this.selectRowsCount = selectRowsCount;

    // Get a table instance.
    try {
      table = ProjectsHBTable.getInstance().getHTable();
    } catch (IOException e) {
      throw new HBaseConnectionException(
          "Could not connect to HBase for projects table.", e);
    }
    table.setAutoFlush(false);
  }

  @Override
  public void insertModels(Collection<ProjectModel> models)
      throws HBaseException {
    ArrayList<Put> puts = new ArrayList<Put>(selectRowsCount);
    Put put = null;
    
    // Add each project to HBase.
    for (ProjectModel project : models) {
      try {
        put = new Put(Bytes.add(
            new byte[] {project.getType()}, project.getId()));
        
        // Default Column Family
        put.add(ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_NAME,
            project.getName().getBytes("UTF-8"));
        if (project.getDescription() != null) {
          put.add(ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_DESCRIPTION,
              project.getDescription().getBytes("UTF-8"));
        }
        if (project.getVersion() != null) {
          put.add(ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_VERSION,
              project.getVersion().getBytes("UTF-8"));
        }
        if (project.getGroup() != null) {
          put.add(ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_GROUP,
              project.getGroup().getBytes("UTF-8"));
        }
        if (project.getPath() != null) {
          put.add(ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_PATH,
              project.getPath().getBytes("UTF-8"));
        }
        put.add(ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_HASSOURCE,
            project.hasSource() ? new byte[] {(byte)1} : new byte[] {(byte)0});
        
        // Metrics Column Family
        if (project.getLoc() != null) {
          put.add(ProjectsHBTable.CF_METRICS, ProjectsHBTable.COL_METRIC_LOC,
              Bytes.toBytes(project.getLoc()));
        }
        if (project.getNwloc() != null) {
          put.add(ProjectsHBTable.CF_METRICS, ProjectsHBTable.COL_METRIC_NWLOC,
              Bytes.toBytes(project.getNwloc()));
        }
        
        puts.add(put);
      } catch (UnsupportedEncodingException e) {
        throw new StringSerializationException(e);
      }
    }
    
    try {
      table.put(puts);
      table.flushCommits();
    } catch (IOException e) {
      throw new HBaseException(e);
    }
  }
  
}
