package edu.nus.soc.sourcerer.model.ddb;

import edu.uci.ics.sourcerer.model.File;


/**
 * Default model for HBase `files` table.
 * 
 * @author Calin-Andrei Burloiu
 * 
 */
public class FileModel extends ModelWithID {
  protected File type;
  protected byte[] projectID;
  
  // Meta
  protected String name;
  protected String path;
  protected byte[] hash;
  protected byte[] jarProjectID;
  
  // Metrics
  protected int loc;
  protected int nwloc;
  
  // Entities
  // TODO
  
}
