package edu.nus.soc.sourcerer.testing.ddb;

import java.util.Collection;
import java.util.Vector;

import org.junit.Before;
import org.junit.Test;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.queries.ProjectModelInserter;
import edu.nus.soc.sourcerer.ddb.queries.ProjectsRetriever;
import edu.nus.soc.sourcerer.ddb.util.ListModelAppender;
import edu.nus.soc.sourcerer.model.ddb.ProjectModel;
import edu.uci.ics.sourcerer.model.Project;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class QueriesTest {
  
  @Before
  public void setUp() {
    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
    dbConf.setTablePrefix("testing_");
  }

  @Test
  public void projectsTest() {
    ProjectModelInserter inserter;
    Collection<ProjectModel> models = new Vector<ProjectModel>(8);
    ProjectModel inputProject = null, outputProject = null;
    ProjectsRetriever retriever;
    
    // Create a project model object to be inserted.
   
    Project type = Project.CRAWLED;
    String name = "Test Project";
    String description = "Carpe diem! YOLO!";
    String version = "1.0.3";
    String group = null;
    String path = "path/to/project";
    byte[] hash = null;
    Boolean hasSource = true;
    Integer loc = 1000;
    Integer nwloc = 900;
  	
  
    
    inputProject = new ProjectModel(type.getValue(), name, description,
        version, group, path, hash, hasSource, loc, nwloc);
  
    models.add(inputProject);
    
    // Compute project ID.
    byte[] id = inputProject.getId();
    
    try {
      // Insert project.
      inserter = new ProjectModelInserter(8);
      inserter.insertModels(models);
      
      // Retrieve the same project.
      retriever = new ProjectsRetriever();
      ListModelAppender<ProjectModel> appender =
          new ListModelAppender<ProjectModel>();
      retriever.retrieveProjects(appender, inputProject.getType(), id);
      outputProject = appender.getList().get(0);
    } catch (HBaseConnectionException e) {
      e.printStackTrace();
    } catch (HBaseException e) {
      e.printStackTrace();
    }
    
    // Check if the inserted project matches the retrieved project.
    assertEquals(inputProject.getName(), outputProject.getName());
    assertEquals(inputProject.getDescription(), outputProject.getDescription());
    assertEquals(inputProject.getVersion(), outputProject.getVersion());
    assertEquals(inputProject.getGroup(), outputProject.getGroup());
    assertEquals(inputProject.getPath(), outputProject.getPath());
    assertEquals(inputProject.hasSource(), outputProject.hasSource());
    assertEquals(inputProject.getLoc(), outputProject.getLoc());
    assertEquals(inputProject.getNwloc(), outputProject.getNwloc());
    assertArrayEquals(inputProject.getHash(), outputProject.getHash());
  }
}
