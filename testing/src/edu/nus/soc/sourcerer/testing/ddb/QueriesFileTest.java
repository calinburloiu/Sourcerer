package edu.nus.soc.sourcerer.testing.ddb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Vector;

import org.junit.Before;
import org.junit.Test;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.queries.FileModelInserter;
import edu.nus.soc.sourcerer.ddb.queries.FilesRetriever;
import edu.nus.soc.sourcerer.ddb.util.ListModelAppender;
import edu.nus.soc.sourcerer.model.ddb.FileModel;
import edu.uci.ics.sourcerer.model.File;

public class QueriesFileTest {
	
	 @Before
	  public void setUp() {
	    DatabaseConfiguration dbConf = DatabaseConfiguration.getInstance();
	    dbConf.setTablePrefix("testing_");
	  }
	 
	 @Test
	  public void fileTest(){
		 FileModelInserter inserter;
		    Collection<FileModel> models = new Vector<FileModel>(9);
		    FileModel inputFile = null, outputFile = null;
		    FilesRetriever retriever;
		    
		    // Create a file model object to be inserted.
		   
		    File type = File.SOURCE;
		    byte[] projectID = {1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6};
		    String name = "Test File";
		    String path = "path/to/file";
		    byte[] hash = {0,1,2,3,4,5,6};
		    byte[] jarProjectID = {1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6};
		    Integer loc = 1000;
		    Integer nwloc = 900;
		  	
		  
		    
		    inputFile = new FileModel(type.getValue(), projectID,name,
		    		path, hash, jarProjectID, loc, nwloc);
		  
		    models.add(inputFile);
		    
		    // Compute file ID.
		    byte[] id = inputFile.getId();
		    
		    try {
		      // Insert project.
		      inserter = new FileModelInserter(9);
		      inserter.insertModels(models);
		      
		      // Retrieve the same file.
		      retriever = new FilesRetriever();
		      
		      ListModelAppender<FileModel> appender =
		          new ListModelAppender<FileModel>();
		      
		      
		      retriever.retrieveFiles(appender, inputFile.getJarProjectID(), inputFile.getType(), id);
		      outputFile = appender.getList().get(0);
		    } catch (HBaseConnectionException e) {
		      e.printStackTrace();
		    } catch (HBaseException e) {
		      e.printStackTrace();
		    }
		    
		    // Check if the inserted file matches the retrieved project.
		    assertEquals(inputFile.getName(), outputFile.getName());
		    assertEquals(inputFile.getPath(), outputFile.getPath());
		    assertEquals(inputFile.getLoc(), outputFile.getLoc());
		    assertEquals(inputFile.getNwloc(), outputFile.getNwloc());
		    assertArrayEquals(inputFile.getHash(), outputFile.getHash());
		    assertArrayEquals(inputFile.getProjectID(), outputFile.getProjectID());
		    assertArrayEquals(inputFile.getJarProjectID(), outputFile.getJarProjectID());
		  
	 }

}
