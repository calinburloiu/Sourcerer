package edu.nus.soc.sourcerer.ddb.queries;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.ProjectsHBTable;
import edu.nus.soc.sourcerer.ddb.util.ModelAppender;
import edu.nus.soc.sourcerer.model.ddb.ProjectModel;
import edu.uci.ics.sourcerer.model.Project;

public class ProjectsRetriever {
  
  protected HTable table;
  
  public ProjectsRetriever() throws HBaseException {
    super();
    
    // Get a table instance.
    try {
      table = ProjectsHBTable.getInstance().getHTable();
    } catch (IOException e) {
      throw new HBaseConnectionException(
          "Could not connect to HBase for projects table.", e);
    }
  }
  
  public static ProjectModel getProjectFromResult(Result result) {
    byte[] row = result.getRow();
    byte type = row[0];
    byte[] projectID = Bytes.tail(row, 16);
    
    byte[] name = result.getValue(
            ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_NAME);
    byte[] description = result.getValue(
            ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_DESCRIPTION);
    byte[] version = result.getValue(
              ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_VERSION);
    byte[] group = result.getValue(
            ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_GROUP);
    byte[] path = result.getValue(
            ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_PATH);
    byte[] hash =
        (type == Project.JAR.getValue() || type == Project.MAVEN.getValue()) ?
        projectID : null;
    byte[] hasSource = result.getValue(
            ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_HASSOURCE);
    byte[] loc = result.getValue(
            ProjectsHBTable.CF_METRICS, ProjectsHBTable.COL_METRIC_LOC);
    byte[] nwloc = result.getValue(
            ProjectsHBTable.CF_METRICS, ProjectsHBTable.COL_METRIC_NWLOC);
    
    return new ProjectModel(projectID, type,
        name == null ? null : Bytes.toString(name),
        description == null ? null : Bytes.toString(description),
        version == null ? null : Bytes.toString(version),
        group == null ? null : Bytes.toString(group),
        path == null ? null : Bytes.toString(path),
        hash,
        hasSource == null ? null : Bytes.toBoolean(hasSource),
        loc == null ? null : Bytes.toInt(loc),
        nwloc == null ? null : Bytes.toInt(nwloc)
        );
  }
  
  public void retrieveProjects(ModelAppender<ProjectModel> appender, Byte type)
      throws HBaseException {
    Scan scan = new Scan(new byte[] {type},
        new byte[] {(byte)(type + 1)});
    ResultScanner scanner = null;
    ProjectModel project = null;
    
    try {
      scanner = table.getScanner(scan);
      
      for (Result result : scanner) {
        project = getProjectFromResult(result);
        appender.add(project);
      }      
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }
  
  public void retrieveProjects(ModelAppender<ProjectModel> appender,
      Byte type, byte[] projectID) 
      throws HBaseException {
    ProjectModel project = null;
    Result result;
    
    if (projectID == null) {
      retrieveProjects(appender, type);
      return;
    }
    
    Get get = new Get(Bytes.add(new byte[] {type}, projectID));
    get.addFamily(ProjectsHBTable.CF_DEFAULT);
    get.addFamily(ProjectsHBTable.CF_METRICS);
    
    try {
      result = table.get(get);
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    }
    
    project = getProjectFromResult(result);
    appender.add(project);
  }

}
