package edu.nus.soc.sourcerer.model.ddb;

import java.lang.reflect.Field;

import edu.nus.soc.sourcerer.util.HumanReadable;
import edu.uci.ics.sourcerer.model.Project;

/**
 * Default model for HBase `projects` table.
 * 
 * @author Calin-Andrei Burloiu
 * 
 */
public class ProjectModel extends ModelWithID {
  protected Project type;
  protected String name;
  protected String description;
  protected String version;
  protected String group;
  protected String path;
  protected boolean hasSource;

  // Line of Code
  protected int loc;
  // Non white space Lines of Code
  protected int nwloc;

  public ProjectModel(byte[] projectID, Project type, String name,
      String description, String version, String group, String path,
      boolean hasSource, int loc, int nwloc) {
    super();
    this.id = projectID;
    this.type = type;
    this.name = name;
    this.description = description;
    this.version = version;
    this.group = group;
    this.path = path;
    this.hasSource = hasSource;
    this.loc = loc;
    this.nwloc = nwloc;
  }
  
  public ProjectModel(Project type, String name,
      String description, String version, String group, String path,
      boolean hasSource, int loc, int nwloc) {
    super();
    this.type = type;
    this.name = name;
    this.description = description;
    this.version = version;
    this.group = group;
    this.path = path;
    this.hasSource = hasSource;
    this.loc = loc;
    this.nwloc = nwloc;
    
    try {
      id = computeId(128, new Field[] {this.getClass().getDeclaredField("name"),
          this.getClass().getDeclaredField("description"),
          this.getClass().getDeclaredField("hasSource"),
          this.getClass().getDeclaredField("nwloc")});
    } catch (SecurityException e) {
      throw new Error("Reflection programming error.", e);
    } catch (NoSuchFieldException e) {
      throw new Error("Reflection programming error.", e);
    }
  }

  public Project getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getVersion() {
    return version;
  }

  public String getGroup() {
    return group;
  }

  public String getPath() {
    return path;
  }

  public boolean hasSource() {
    return hasSource;
  }

  public int getLoc() {
    return loc;
  }

  public int getNwloc() {
    return nwloc;
  }

  @Override
  public String toString() {
    return "project " + name + " ("
        + HumanReadable.byteArrayToHexString(id) + ")";
  }
  
//  public static void main(String args[]) {
//    ProjectDDB prj = new ProjectDDB(Project.CRAWLED, "abc", "A", "", "", "",
//        false, 0xCAFEBABE, 0x01020304);
//    System.out.println(HumanReadable.byteArrayToHexString(prj.getId()));
//  }
}
