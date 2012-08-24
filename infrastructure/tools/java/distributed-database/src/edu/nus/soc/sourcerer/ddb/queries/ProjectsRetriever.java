package edu.nus.soc.sourcerer.ddb.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.ProjectsHBTable;
import edu.nus.soc.sourcerer.ddb.util.ModelAppender;
import edu.nus.soc.sourcerer.model.ddb.Model;
import edu.nus.soc.sourcerer.model.ddb.ProjectModel;
import edu.nus.soc.sourcerer.util.EnumUtil;
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
  
  public static ProjectModel resultToProjectModel(Result result) {
    if (result == null)
      return null;
    
    byte[] row = result.getRow();
    if (row == null)
      return null;
    
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
  
  public void retrieveProjects(ModelAppender<Model> appender, Byte type)
      throws HBaseException {
    Scan scan = null;
    ResultScanner scanner = null;
    ProjectModel project = null;
    
    // If type is null scan all projects.
    if (type == null) 
      scan = new Scan();
    else
      scan = new Scan(new byte[] {type},
          new byte[] {(byte)(type + 1)});
    
    scan.addFamily(ProjectsHBTable.CF_DEFAULT);
    scan.addFamily(ProjectsHBTable.CF_METRICS);
    
    try {
      scanner = table.getScanner(scan);
      
      for (Result result : scanner) {
        project = resultToProjectModel(result);
        appender.add(project);
      }      
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    } finally {
      if (scanner != null)
        scanner.close();
    }
  }
  
  public void retrieveProjects(ModelAppender<Model> appender,
      Byte type, byte[] projectID) 
      throws HBaseException {
    ProjectModel project = null;
    
    // Scan all projects of a type.
    if (projectID == null) {
      retrieveProjects(appender, type);
      return;
    }
    
    // If type is null search project of all types with that ID.
    Project[] types = null;
    if (type != null) {
      types = new Project[] { 
          (Project) EnumUtil.getEnumByValue(Project.values(), new Byte(type)) };
    }
    else {
      types = Project.values();
    }
    
    List<Row> batch = new ArrayList<Row>(types.length);
    Object[] results = new Object[types.length];
    
    for (Project crtType : types) {
      Get get = new Get(Bytes.add(new byte[] {crtType.getValue()}, projectID));
      get.addFamily(ProjectsHBTable.CF_DEFAULT);
      get.addFamily(ProjectsHBTable.CF_METRICS);
      batch.add(get);
    }
    
    try {
      table.batch(batch, results);
    } catch (IOException e) {
      throw new HBaseException(e.getMessage(), e);
    } catch (InterruptedException e) {}
    
    // Only one project can be found (ID is unique).
    for (Object result : results) {
      if (result != null && !((Result) result).isEmpty()) {
        project = resultToProjectModel((Result) result);
        appender.add(project);
        break;
      }
    }
  }

}
