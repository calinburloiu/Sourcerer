package edu.nus.soc.sourcerer.ddb.queries;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.tables.ProjectsHBTable;
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
  
  public ProjectModel retrieveProjects(Project type, byte[] projectID) 
      throws HBaseException {
    ProjectModel project = null;
    Result result;
    
    if (projectID != null) {
      
      Get get = new Get(Bytes.add(new byte[] {type.getValue()}, projectID));
      get.addFamily(ProjectsHBTable.CF_DEFAULT);
      get.addFamily(ProjectsHBTable.CF_METRICS);
      
      try {
        result = table.get(get);
      } catch (IOException e) {
        throw new HBaseException(e.getMessage(), e);
      }
      
      byte[] hash = (type == Project.JAR || type == Project.MAVEN) ?
          projectID : null;
      project = new ProjectModel(projectID, type,
          Bytes.toString(result.getValue(
              ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_NAME)),
          Bytes.toString(result.getValue(
              ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_DESCRIPTION)),
          Bytes.toString(result.getValue(
                ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_VERSION)),
          Bytes.toString(result.getValue(
              ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_GROUP)),
          Bytes.toString(result.getValue(
              ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_PATH)),
          hash,
          Bytes.toBoolean(result.getValue(
              ProjectsHBTable.CF_DEFAULT, ProjectsHBTable.COL_HASSOURCE)),
          Bytes.toInt(result.getValue(
              ProjectsHBTable.CF_METRICS, ProjectsHBTable.COL_METRIC_LOC)),
          Bytes.toInt(result.getValue(
              ProjectsHBTable.CF_METRICS, ProjectsHBTable.COL_METRIC_NWLOC))
          );
    }
    
    return project;
  }

}
