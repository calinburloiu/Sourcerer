package edu.nus.soc.sourcerer.ddb.tools;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Vector;

import edu.nus.soc.sourcerer.ddb.DatabaseConfiguration;
import edu.nus.soc.sourcerer.ddb.HBaseConnectionException;
import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.ddb.queries.EntityModelInserter;
import edu.nus.soc.sourcerer.ddb.queries.FileModelInserter;
import edu.nus.soc.sourcerer.ddb.queries.ModelInserter;
import edu.nus.soc.sourcerer.ddb.queries.ProjectModelInserter;
import edu.nus.soc.sourcerer.ddb.queries.RelationModelInserter;
import edu.nus.soc.sourcerer.model.ddb.EntityModel;
import edu.nus.soc.sourcerer.model.ddb.FileModel;
import edu.nus.soc.sourcerer.model.ddb.Model;
import edu.nus.soc.sourcerer.model.ddb.ProjectModel;
import edu.nus.soc.sourcerer.model.ddb.RelationModel;
import edu.nus.soc.sourcerer.util.Serialization;
import edu.uci.ics.sourcerer.db.util.DatabaseConnection;
import edu.uci.ics.sourcerer.db.util.QueryExecutor;

import edu.uci.ics.sourcerer.db.schema.ProjectsTable;
import edu.uci.ics.sourcerer.db.schema.FilesTable;
import edu.uci.ics.sourcerer.db.schema.EntitiesTable;
import edu.uci.ics.sourcerer.db.schema.RelationsTable;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.File;
import edu.uci.ics.sourcerer.model.Project;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;

import static edu.nus.soc.sourcerer.ddb.Commons.LOG;

/**
 * This class imports data from SourcererDB MySQL database to SourcererDDC
 * HBase database.
 * 
 * It was used during development while porting the database from MySQL to
 * HBase when a distributed extractor was not yet implemented and the new
 * database needed to be populated.
 * 
 * @author Calin-Andrei Burloiu
 *
 */
public class MySQLImporter {
  
  protected DatabaseConfiguration databaseConfiguration = null;
  protected final int SELECT_ROWS_COUNT; 
  
  protected DatabaseConnection connection = null;
  protected QueryExecutor executor = null;
  
  protected Collection<Task<? extends Model>> tasks = null;

  public MySQLImporter(DatabaseConfiguration databaseConfiguration,
      int selectRowsCount) {
    super();
    
    this.databaseConfiguration = databaseConfiguration;
    this.SELECT_ROWS_COUNT = selectRowsCount;
    
    connection = new DatabaseConnection();
    connection.open();
    executor = new QueryExecutor(connection.getConnection());
    
    tasks = new Vector<Task<? extends Model>>(4);
    tasks.add(new ProjectsImporter());
    tasks.add(new FilesImporter());
    tasks.add(new EntitiesImporter());
    tasks.add(new RelationsImporter());
  }

  public void start() throws SQLException, HBaseException {
    for (Task<? extends Model> task : tasks) {
      LOG.info("Importing " + task.getTaskMessage() + "...");
      task.execute();
      LOG.info("Importing " + task.getTaskMessage() + " completed.");
    }
  }

  protected abstract class Task<E extends Model> {
    protected abstract String getTaskMessage();
    protected abstract String getSQLQuery(int startID);
    protected abstract E getHBaseModel(ResultSet result) throws SQLException;
    protected abstract int getSQLId(ResultSet result) throws SQLException;
    protected abstract ModelInserter<E> getHBaseModelInserter()
        throws HBaseConnectionException;
    
    public void execute() throws SQLException, HBaseException {
      Statement statement = connection.getConnection().createStatement();
      ResultSet results;
      Collection<E> models = null;
      E model = null;
      ModelInserter<E> modelInserter = getHBaseModelInserter();
      int startID = 0, count = 0;
      
      // for each select
      while (true) {
        results = statement.executeQuery(getSQLQuery(startID));
        
        if (!results.next())
          break;
        
        models = new Vector<E>(SELECT_ROWS_COUNT);
        
        // for each row from the select
        do {
          model = getHBaseModel(results);
          models.add(model);
          startID = getSQLId(results);
          count++;
        } while (results.next());
        
        modelInserter.insertModels(models);
        LOG.debug("Inserted " + count + " models to " + getTaskMessage()
            + " tables.");
      }
      
      LOG.debug("Finished inserting " + count + " models to " + getTaskMessage()
            + " tables.");
    }
  }
  
  protected class ProjectsImporter extends Task<ProjectModel> {
    @Override
    protected String getTaskMessage() {
      return "projects";
    }

    @Override
    protected String getSQLQuery(int startID) {
      return  "SELECT p.*, m1.value loc, m2.value nwloc "
            + "FROM projects p "
            + "LEFT JOIN project_metrics m1 "
            + "  ON p.project_id = m1.project_id "
            + "    AND m1.metric_type = 'LINES_OF_CODE' "
            + "LEFT JOIN project_metrics m2 "
            + "  ON p.project_id = m2.project_id "
            + "    AND m2.metric_type = 'NON_WHITESPACE_LINES_OF_CODE' "
            + "WHERE p.project_id > " + startID + " "
            + "ORDER BY p.project_id "
            + "LIMIT " + SELECT_ROWS_COUNT + ";";
    }
    
    @Override
    protected ProjectModel getHBaseModel(ResultSet result) throws SQLException {
      String projectType = 
          result.getString(ProjectsTable.PROJECT_TYPE.getName());
      Integer loc = (Integer)result.getObject("loc");
      Integer nwloc = (Integer)result.getObject("nwloc");
      
      ProjectModel project = new ProjectModel(
          projectType == null ? Project.UNKNOWN.getValue()
              : Project.valueOf(projectType).getValue(), 
          result.getString(ProjectsTable.NAME.getName()),
          result.getString(ProjectsTable.DESCRIPTION.getName()), 
          result.getString(ProjectsTable.VERSION.getName()), 
          result.getString(ProjectsTable.GROUP.getName()), 
          result.getString(ProjectsTable.PATH.getName()),
          Serialization.hexStringToByteArray(
              result.getString(ProjectsTable.HASH.getName())), 
          result.getBoolean(ProjectsTable.HAS_SOURCE.getName()), 
          loc, nwloc);
      
      return project;
    }
    
    @Override
    protected int getSQLId(ResultSet result) throws SQLException {
      return result.getInt(ProjectsTable.PROJECT_ID.getName());
    }
    
    @Override
    protected ModelInserter<ProjectModel> getHBaseModelInserter()
        throws HBaseConnectionException {
      return new ProjectModelInserter(SELECT_ROWS_COUNT);
    }
  }
  
  protected class FilesImporter extends Task<FileModel> {
    @Override
    protected String getTaskMessage() {
      return "files";
    }

    @Override
    protected String getSQLQuery(int startID) {
      return  "SELECT f.*, p.project_type, p.name project_name, "
              + "p.path project_path, p.hash project_hash, "
              + "m1.value loc, m2.value nwloc "
          + "FROM files f "
          + "LEFT JOIN projects p "
          + "  ON p.project_id = f.project_id "
          + "LEFT JOIN file_metrics m1 "
          + "  ON f.file_id = m1.file_id "
          + "    AND m1.metric_type = 'LINES_OF_CODE' "
          + "LEFT JOIN file_metrics m2 "
          + "  ON f.file_id = m2.file_id "
          + "    AND m2.metric_type = 'NON_WHITESPACE_LINES_OF_CODE' "
          + "WHERE f.file_id > " + startID + " "
          + "ORDER BY f.file_id "
          + "LIMIT " + SELECT_ROWS_COUNT + ";";
    }

    @Override
    protected FileModel getHBaseModel(ResultSet result) throws SQLException {
      String fileType = 
          result.getString(FilesTable.FILE_TYPE.getName());
      Integer loc = (Integer)result.getObject("loc");
      Integer nwloc = (Integer)result.getObject("nwloc");
      
      // Compute project ID.
      String projectType = result.getString("project_type");
      String projectPath = result.getString("project_path");
      byte[] projectHash =
          Serialization.hexStringToByteArray(result.getString("project_hash"));
      byte[] projectID = new ProjectModel(
          projectType == null ? Project.UNKNOWN.getValue()
              : Project.valueOf(projectType).getValue(),
          result.getString("project_name"), projectPath,
          projectHash).getId();
      
      // File path shouldn't be relative to project path so we concatenate
      // project path with file path if applicable.
      String path = null;
      String filePath = result.getString(FilesTable.PATH.getName());
      if (filePath != null)
        path = (projectPath == null ? "" : projectPath) + filePath;
      
      FileModel project = new FileModel(
          fileType == null ? File.UNKNOWN.getValue()
              : File.valueOf(fileType).getValue(), 
          projectID, result.getString(FilesTable.NAME.getName()), path,
          Serialization.hexStringToByteArray(result.getString(
              FilesTable.HASH.getName())), projectHash, loc, nwloc);
      
      return project;
    }

    @Override
    protected int getSQLId(ResultSet result) throws SQLException {
      return result.getInt(FilesTable.FILE_ID.getName());
    }

    @Override
    protected ModelInserter<FileModel> getHBaseModelInserter() throws HBaseConnectionException {
      return new FileModelInserter(SELECT_ROWS_COUNT);
    }
    
    
  }

  protected class EntitiesImporter extends Task<EntityModel> {

    @Override
    protected String getTaskMessage() {
      return "entities";
    }

    @Override
    protected String getSQLQuery(int startID) {
      return "SELECT e.*, m1.value loc, m2.value nwloc, p.project_type, "
              + "p.name project_name, p.path project_path, "
              + "p.hash project_hash, f.file_type, f.name file_name, "
              + "f.path file_path "
          + "FROM `entities` e "
          + "  LEFT JOIN projects p "
          + "    ON e.project_id = p.project_id "
          + "  LEFT JOIN files f "
          + "    ON e.file_id = f.file_id "
          + "  LEFT JOIN entity_metrics m1 "
          + "    ON e.entity_id = m1.entity_id "
          + "      AND m1.metric_type = 'LINES_OF_CODE' "
          + "  LEFT JOIN entity_metrics m2 "
          + "    ON e.entity_id = m2.entity_id "
          + "      AND m2.metric_type = 'NON_WHITESPACE_LINES_OF_CODE' "
          + "WHERE e.entity_id > " + startID + " "
          + "ORDER BY e.entity_id "
          + "LIMIT " + SELECT_ROWS_COUNT + ";";
    }

    @Override
    protected EntityModel getHBaseModel(ResultSet result) throws SQLException {
      // Compute project ID.
      String projectType = result.getString("project_type");
      String projectPath = result.getString("project_path");
      byte[] projectHash =
          Serialization.hexStringToByteArray(result.getString("project_hash"));
      byte[] projectID = new ProjectModel(
          projectType == null ? Project.UNKNOWN.getValue()
              : Project.valueOf(projectType).getValue(),
          result.getString("project_name"), projectPath,
          projectHash).getId();
      
      // Compute file ID.
      String fileType = result.getString("file_type");
      String filePath = (projectPath == null ? "" : projectPath)
          + result.getString("file_path");
      String fileName = result.getString("file_name");
      byte[] fileID = new FileModel(
          fileType == null ? File.UNKNOWN.getValue()
              : File.valueOf(fileType).getValue(),
          fileName, filePath).getId();
      
      // Entity type
      String entityType = result.getString(EntitiesTable.ENTITY_TYPE.getName());
      
      // Integer properties
      Long lModifiers = 
          (Long)result.getObject(EntitiesTable.MODIFIERS.getName());
      Long lMulti =
          (Long)result.getObject(EntitiesTable.MULTI.getName());
      Long lOffset =
          (Long)result.getObject(EntitiesTable.OFFSET.getName());
      Long lLength =
          (Long)result.getObject(EntitiesTable.LENGTH.getName());
      Integer loc =
          (Integer)result.getObject("loc");
      Integer nwloc =
          (Integer)result.getObject("nwloc");
      
      EntityModel entity = new EntityModel(
          entityType == null ? Entity.UNKNOWN.getValue()
              : Entity.valueOf(entityType).getValue(),
          result.getString(EntitiesTable.FQN.getName()), projectID, fileID,
          lModifiers == null ? null : lModifiers.intValue(),
          lMulti == null ? null : lMulti.intValue(),
          lOffset == null ? null : lOffset.intValue(),
          lLength == null ? null : lLength.intValue(),
          loc, nwloc,
          fileType == null ? File.UNKNOWN.getValue()
              : File.valueOf(fileType).getValue());
      
      return entity;
    }

    @Override
    protected int getSQLId(ResultSet result) throws SQLException {
      return result.getInt(EntitiesTable.ENTITY_ID.getName());
    }

    @Override
    protected ModelInserter<EntityModel> getHBaseModelInserter()
        throws HBaseConnectionException {
      return new EntityModelInserter(SELECT_ROWS_COUNT);
    }
    
  }

  protected class RelationsImporter extends Task<RelationModel> {

    @Override
    protected String getTaskMessage() {
      return "relations";
    }

    @Override
    protected String getSQLQuery(int startID) {
      return "SELECT r.*, "
              + "p.project_type project_type, p.name project_name, "
              + "p.path project_path, p.hash project_hash, "
              + "f.file_type file_type, f1.name file_name, f1.path file_path, "
              + "e1.entity_type lhs_entity_type, e1.fqn lhs_fqn, "
              + "e1.project_id lhs_project_id, e1.file_id lhs_file_id, "
              + "e1.modifiers lhs_modifiers, e1.multi lhs_multi, "
              + "e1.offset lhs_offset, e1.length lhs_length, "
              + "p1.project_type lhs_project_type, p1.name lhs_project_name, "
              + "p1.path lhs_project_path, p1.hash lhs_project_hash, "
              + "f1.file_type lhs_file_type, f1.name lhs_file_name, "
              + "f1.path lhs_file_path, " 
              + "e2.entity_type rhs_entity_type, e2.fqn rhs_fqn, "
              + "e2.project_id rhs_project_id, e2.file_id rhs_file_id, "
              + "e2.modifiers rhs_modifiers, e2.multi rhs_multi, "
              + "e2.offset rhs_offset, e2.length rhs_length, "
              + "p2.project_type rhs_project_type, p2.name rhs_project_name, "
              + "p2.path rhs_project_path, p2.hash rhs_project_hash, "
              + "f2.file_type rhs_file_type, f2.name rhs_file_name, "
              + "f2.path rhs_file_path "
          + "FROM relations r "
          + "  JOIN projects p "
          + "    ON r.project_id = p.project_id "
          + "  LEFT JOIN files f "
          + "    ON r.file_id = f.file_id "
          + "  JOIN entities e1 "
          + "    ON r.lhs_eid = e1.entity_id "
          + "  LEFT JOIN projects p1 "
          + "    ON e1.project_id = p1.project_id "
          + "  LEFT JOIN files f1 "
          + "    ON e1.file_id = f1.file_id "
          + "  JOIN entities e2 "
          + "    ON r.rhs_eid = e2.entity_id "
          + "  LEFT JOIN projects p2 "
          + "    ON e2.project_id = p2.project_id "
          + "  LEFT JOIN files f2 "
          + "    ON e2.file_id = f2.file_id "
          + "WHERE r.relation_id > " + startID + " "
          + "ORDER BY r.relation_id "
          + "LIMIT " + SELECT_ROWS_COUNT + ";";
    }
    
    protected EntityModel getHBaseEntityModel(ResultSet result,
        String colPrefix) throws SQLException {
      // Compute project ID.
      String projectType = result.getString(colPrefix + "project_type");
      String projectPath = result.getString(colPrefix + "project_path");
      byte[] projectHash =
          Serialization.hexStringToByteArray(
              result.getString(colPrefix + "project_hash"));
      byte[] projectID = new ProjectModel(
          projectType == null ? Project.UNKNOWN.getValue()
              : Project.valueOf(projectType).getValue(),
          result.getString(colPrefix + "project_name"), projectPath,
          projectHash).getId();
      
      // Compute file ID.
      String fileType = result.getString(colPrefix + "file_type");
      String filePath = (projectPath == null ? "" : projectPath)
          + result.getString(colPrefix + "file_path");
      String fileName = result.getString(colPrefix + "file_name");
      byte[] fileID = new FileModel(
          fileType == null ? File.UNKNOWN.getValue()
              : File.valueOf(fileType).getValue(),
          fileName, filePath).getId();
      
      // Entity type
      String entityType = result.getString(
          colPrefix + EntitiesTable.ENTITY_TYPE.getName());
      
      // Integer properties
      Long lModifiers = 
          (Long)result.getObject(colPrefix + EntitiesTable.MODIFIERS.getName());
      Long lMulti =
          (Long)result.getObject(colPrefix + EntitiesTable.MULTI.getName());
      Long lOffset =
          (Long)result.getObject(colPrefix + EntitiesTable.OFFSET.getName());
      Long lLength =
          (Long)result.getObject(colPrefix + EntitiesTable.LENGTH.getName());
      
      EntityModel entity = new EntityModel(
          entityType == null ? Entity.UNKNOWN.getValue()
              : Entity.valueOf(entityType).getValue(),
          result.getString(colPrefix + EntitiesTable.FQN.getName()), projectID,
          fileID, lModifiers == null ? null : lModifiers.intValue(),
          lMulti == null ? null : lMulti.intValue(),
          lOffset == null ? null : lOffset.intValue(),
          lLength == null ? null : lLength.intValue(),
          null, null,
          fileType == null ? File.UNKNOWN.getValue()
              : File.valueOf(fileType).getValue());
      
      return entity;
    }

    @Override
    protected RelationModel getHBaseModel(ResultSet result) throws SQLException {
      EntityModel sourceEntity = getHBaseEntityModel(result, "lhs_");
      EntityModel targetEntity = getHBaseEntityModel(result, "rhs_");
      
      // Relation kind
      String relationType = result.getString(
          RelationsTable.RELATION_TYPE.getName());
      String relationClass = result.getString(
          RelationsTable.RELATION_CLASS.getName());
      
      // Compute project ID.
      String projectType = result.getString("project_type");
      String projectPath = result.getString("project_path");
      byte[] projectHash =
          Serialization.hexStringToByteArray(
              result.getString("project_hash"));
      byte[] projectID = new ProjectModel(
          projectType == null ? Project.UNKNOWN.getValue()
              : Project.valueOf(projectType).getValue(),
          result.getString("project_name"), projectPath,
          projectHash).getId();
      
      // Compute file ID.
      String fileType = result.getString("file_type");
      String filePath = (projectPath == null ? "" : projectPath)
          + result.getString("file_path");
      String fileName = result.getString("file_name");
      byte[] fileID = new FileModel(
          fileType == null ? File.UNKNOWN.getValue()
              : File.valueOf(fileType).getValue(),
          fileName, filePath).getId();
      
      Long lOffset =
          (Long)result.getObject(EntitiesTable.OFFSET.getName());
      Long lLength =
          (Long)result.getObject(EntitiesTable.LENGTH.getName());
      
      Byte relationKind = (byte) ((relationType == null
          ? Relation.UNKNOWN.getValue()
              : Relation.valueOf(relationType).getValue())
          | (relationClass == null
              ? RelationClass.UNKNOWN.getValue()
                  : RelationClass.valueOf(relationClass).getValue()));
      
      RelationModel relation = new RelationModel(relationKind,
          sourceEntity.getId(), targetEntity.getId(), projectID, fileID,
          lOffset == null ? null : lOffset.intValue(),
          lLength == null ? null : lLength.intValue(),
          sourceEntity.getType(), targetEntity.getType(),
          fileType == null ? File.UNKNOWN.getValue()
              : File.valueOf(fileType).getValue());
      
      return relation;
    }

    @Override
    protected int getSQLId(ResultSet result) throws SQLException {
      return result.getInt(RelationsTable.RELATION_ID.getName());
    }

    @Override
    protected ModelInserter<RelationModel> getHBaseModelInserter()
        throws HBaseConnectionException {
      return new RelationModelInserter(SELECT_ROWS_COUNT);
    }

  }
}
