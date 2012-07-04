package edu.nus.soc.sourcerer.drepo.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static edu.uci.ics.sourcerer.util.io.Logging.logger;

import org.apache.commons.io.FileUtils;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.queries.SourceModelInserter;
import edu.nus.soc.sourcerer.model.ddb.SourceModel;

public class RepoImporter {
  protected File inputRepo = null;
  protected DatabaseConfiguration databaseConfiguration = null;
  protected final int READ_FILES_COUNT;

  public RepoImporter(File inputRepo, 
      DatabaseConfiguration databaseConfiguration, int readFilesCount) {
    super();
    this.inputRepo = inputRepo;
    this.databaseConfiguration = databaseConfiguration;
    this.READ_FILES_COUNT = readFilesCount;
  }
  
  public void start() throws HBaseException{
    logger.info("Importing source (*.java) files...");
    importSources();
  }
  
  public void importSources() throws HBaseException {
    Iterator<?> fileIt = FileUtils.iterateFiles(inputRepo,
        new String[] {"java"}, true);
    SourceModel source = null;
    Collection<SourceModel> sources =
        new ArrayList<SourceModel>(READ_FILES_COUNT);
    SourceModelInserter inserter = null;
    int count = 0;
    File file = null;
    String fileName = null;
    
    while (fileIt.hasNext()) {
      file = (File)fileIt.next();
      
      // TODO use correct path
      try {
        if (file.getAbsolutePath().toString().indexOf(
            inputRepo.getAbsolutePath().toString()) == 0) {
          fileName = file.getAbsolutePath().toString().substring(
              inputRepo.getAbsolutePath().toString().length() + 1);
        }
        else {
          fileName = file.getAbsolutePath().toString();
        }
        source = new SourceModel(fileName,
            readFileContent(file));
        sources.add(source);
      } catch (IOException e) {
        logger.severe("An error occured while reading file '"
            + file.getAbsolutePath() + "': " + e.getMessage());
      } finally {
        count++;
        if (count == READ_FILES_COUNT || !fileIt.hasNext()) {
          inserter = new SourceModelInserter(READ_FILES_COUNT);
          inserter.insertModels(sources);
          
          count = 0;
          source = null;
          sources = new ArrayList<SourceModel>(READ_FILES_COUNT);
        }
      }
    }
  }
  
  public static String readFileContent(File file) throws IOException {
    StringBuffer fileData = new StringBuffer(4096);
    BufferedReader reader = new BufferedReader(new FileReader(file));
    char[] buf = new char[4096];
    int numRead = 0;
    
    while ((numRead = reader.read(buf)) != -1) {
      fileData.append(String.valueOf(buf, 0, numRead));
    }
    
    reader.close();
    return fileData.toString();
  }
  
  // FIXME
  public static void main(String args[]) throws IOException {
    File file = new File("/home/calin/tmp/sample-repo/48/75/content/openjmx/src/tools/openjmx/tools/naming");
    System.out.println(file.getAbsolutePath());
    
  }

}
