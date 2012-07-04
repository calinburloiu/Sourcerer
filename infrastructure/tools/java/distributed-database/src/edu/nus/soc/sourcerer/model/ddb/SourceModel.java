package edu.nus.soc.sourcerer.model.ddb;

public class SourceModel implements Model {
  protected String fileName;
  protected String content;
  
  public SourceModel(String fileName, String content) {
    super();
    this.fileName = fileName;
    this.content = content;
  }

  public String getFileName() {
    return fileName;
  }

  public String getContent() {
    return content;
  }
  
}
