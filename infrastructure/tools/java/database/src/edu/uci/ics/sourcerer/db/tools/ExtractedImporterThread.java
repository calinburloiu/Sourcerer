package edu.uci.ics.sourcerer.db.tools;

import static edu.uci.ics.sourcerer.util.io.Logging.logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import edu.uci.ics.sourcerer.db.util.DatabaseConnection;
import edu.uci.ics.sourcerer.model.Comment;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.File;
import edu.uci.ics.sourcerer.model.Relation;
import edu.uci.ics.sourcerer.model.RelationClass;
import edu.uci.ics.sourcerer.model.db.MediumEntityDB;
import edu.uci.ics.sourcerer.model.db.SmallEntityDB;
import edu.uci.ics.sourcerer.model.extracted.CommentEX;
import edu.uci.ics.sourcerer.model.extracted.EntityEX;
import edu.uci.ics.sourcerer.model.extracted.FileEX;
import edu.uci.ics.sourcerer.model.extracted.ImportEX;
import edu.uci.ics.sourcerer.model.extracted.LocalVariableEX;
import edu.uci.ics.sourcerer.model.extracted.ModelEX;
import edu.uci.ics.sourcerer.model.extracted.ProblemEX;
import edu.uci.ics.sourcerer.model.extracted.RelationEX;
import edu.uci.ics.sourcerer.model.extracted.UsedJarEX;
import edu.uci.ics.sourcerer.model.metrics.Metric;
import edu.uci.ics.sourcerer.repo.extracted.Extracted;
import edu.uci.ics.sourcerer.util.Counter;
import edu.uci.ics.sourcerer.util.Helper;
import edu.uci.ics.sourcerer.util.Pair;
import edu.uci.ics.sourcerer.util.TimeCounter;
import edu.uci.ics.sourcerer.util.io.FileUtils;
import edu.uci.ics.sourcerer.util.io.Logging;

public abstract class ExtractedImporterThread extends ParallelDatabaseImporterThread {
  private java.io.File tempDir;
  private Map<String, Integer> fileMap;
  private Map<String, Ent> entityMap;
  private Set<String> pendingEntities;
  private Collection<RelationEX> newTypeRelations;
  
  private SynchronizedUnknownsMap unknowns;
  
  protected RelationClassifier classifier;
  
  private String inClause;
  
  protected ExtractedImporterThread(DatabaseConnection connection) {
    super(connection);
  }
  
  protected ExtractedImporterThread(DatabaseConnection connection, SynchronizedUnknownsMap unknowns) {
    this(connection);
    this.unknowns = unknowns;
  }
 
  @Override
  public final void run() {
    Logging.addThreadLogger();
    tempDir = FileUtils.getTempDir();
    fileMap = Helper.newHashMap();
    entityMap = Helper.newHashMap();
    pendingEntities = Helper.newHashSet();
    newTypeRelations = Helper.newArrayList();
    
    doImport();
    
    close();
    closeConnection();
    Logging.removeThreadLogger();
  }
  
  protected abstract void doImport();
  
  protected void clearMaps() {
    fileMap.clear();
    entityMap.clear();
  }
  
  protected void insertFiles(Extracted extracted, Integer projectID) {
    logger.info("  Inserting files...");
    
    TimeCounter counter = new TimeCounter();
    
    logger.info("    Processing files...");
    filesTable.initializeInserter(tempDir);
    for (FileEX file : extracted.getFileReader()) {
      filesTable.insert(file, projectID);
      counter.increment();
    }
    
    logger.info(counter.reportTimeAndCount(6, "files processed"));
    
    counter.lap();
    
    logger.info("    Performing db insert...");
    filesTable.flushInserts();
    logger.info(counter.reportTimeAndTotalCount(6, "files inserted"));

    logger.info(counter.reportTotalTimeAndCount(4, "files processed and inserted"));
  }
  
  protected void loadFileMap(Integer projectID) {
    logger.info("  Populating file map...");

    TimeCounter counter = new TimeCounter();
    
    fileQueries.populateFileMap(fileMap, projectID);
    
    counter.setCount(fileMap.size());
    logger.info(counter.reportTimeAndCount(4, "files loaded"));
  }
  
  protected void insertFileMetrics(Extracted extracted, Integer projectID) {
    logger.info("  Inserting file metrics...");
    
    TimeCounter counter = new TimeCounter();
    
    logger.info("    Processing file metrics...");
    
    Map<Metric, Counter<Metric>> projectMetrics = Helper.newEnumMap(Metric.class);
    
    fileMetricsTable.initializeInserter(tempDir);
    for (FileEX file : extracted.getFileReader()) {
      if (file.getType() == File.SOURCE) {
        Integer fileID = fileMap.get(file.getPath());
        if (fileID == null) {
          logger.log(Level.SEVERE, "Unknown file: " + file.getPath());
        } else {
          for (Map.Entry<Metric, Integer> metric : file.getMetrics().getMetricValues()) {
            fileMetricsTable.insert(projectID, fileID, metric.getKey(), metric.getValue());
            Counter<Metric> count = projectMetrics.get(metric.getKey());
            if (count == null) {
              count = new Counter<Metric>(metric.getKey());
              projectMetrics.put(metric.getKey(), count);
            }
            count.add(metric.getValue());
            counter.increment();
          }
        }
      }
    }
    
    logger.info(counter.reportTimeAndCount(6, "file metrics processed"));
    
    counter.lap();
    
    logger.info("    Performing db insert...");
    fileMetricsTable.flushInserts();
    logger.info(counter.reportTimeAndTotalCount(6, "file metrics inserted"));

    counter.lap();
    
    logger.info("    Inserting project metrics...");
    projectMetricsTable.initializeInserter(tempDir);
    for (Counter<Metric> count : projectMetrics.values()) {
      projectMetricsTable.insert(projectID, count.getObject(), count.getCount());
    }
    projectMetricsTable.flushInserts();
    logger.info(counter.reportTime(6, "Project metrics inserted"));
    
    logger.info(counter.reportTotalTimeAndCount(4, "files processed and inserted"));
  }
  
  protected void insertProblems(Extracted extracted, Integer projectID) {
    logger.info("  Inserting problems...");

    TimeCounter counter = new TimeCounter();
    
    logger.info("    Processing problems...");
    
    problemsTable.initializeInserter(tempDir);
    for (ProblemEX problem : extracted.getProblemReader()) {
      Integer fileID = fileMap.get(problem.getRelativePath());
      if (fileID == null) {
        logger.log(Level.SEVERE, "Unknown file: " + problem.getRelativePath() + " for " + problem);
      } else {
        problemsTable.insert(problem, projectID, fileID);
        counter.increment();
      }
    }
    
    logger.info(counter.reportTimeAndCount(6, "problems processed"));
    
    counter.lap();
    
    logger.info("    Performing db insert...");
    problemsTable.flushInserts();
    logger.info(counter.reportTimeAndTotalCount(6, "problems inserted"));
    
    logger.info(counter.reportTotalTimeAndCount(4, "problems processed and inserted"));
  }
  
  protected void insertEntities(Extracted extracted, Integer projectID) {
    logger.info("  Inserting entities....");

    TimeCounter counter = new TimeCounter();
    
    logger.info("    Processing entities....");
    
    entitiesTable.initializeInserter(tempDir);
    logger.info("    Processing from entities file...");
    for (EntityEX entity : extracted.getEntityReader()) {
      Integer fileID = getFileID(entity.getPath(), entity);
      if (fileID == null && !entity.getType().isPackage()) {
        logger.log(Level.SEVERE, "Unknown file: " + entity.getPath());
      }
      entitiesTable.insert(entity, projectID, fileID);
        
      counter.increment();
    }
    
    logger.info(counter.reportTimeAndCount(6, "entities processed"));
    
    counter.lap();
    
    logger.info("    Performing db insert...");
    entitiesTable.flushInserts();
    logger.info(counter.reportTimeAndTotalCount(6, "entities inserted"));
    
    logger.info(counter.reportTotalTimeAndCount(4, "entities processed and inserted"));
  }
  
  protected void insertEntityMetrics(Extracted extracted, Integer projectID) {
    logger.info("  Inserting entity metrics...");
    
    TimeCounter counter = new TimeCounter();
    
    logger.info("    Processing entity metrics...");
    
    entityMetricsTable.initializeInserter(tempDir);
    for (EntityEX entity: extracted.getEntityReader()) {
      Integer fileID = fileMap.get(entity.getPath());
      SmallEntityDB eid = getEid(entity.getFqn(), projectID);
      if (fileID == null) {
        logger.log(Level.SEVERE, "Unknown file: " + entity.getPath());
      } else if (eid == null) {
        logger.log(Level.SEVERE, "Unknown entity: " + entity.getFqn());
      } else if (!eid.getProjectID().equals(projectID)) {
        logger.log(Level.SEVERE, "Incorrect project: " + entity.getFqn());
      } else {
        for (Map.Entry<Metric, Integer> metric : entity.getMetrics().getMetricValues()) {
          entityMetricsTable.insert(projectID, fileID, eid.getEntityID(), metric.getKey(), metric.getValue());
          counter.increment();
        }
      } 
    }
    
    logger.info(counter.reportTimeAndCount(6, "entity metrics processed"));
    
    counter.lap();
    
    logger.info("    Performing db insert...");
    entityMetricsTable.flushInserts();
    logger.info(counter.reportTimeAndTotalCount(6, "entity metrics inserted"));

    logger.info(counter.reportTotalTimeAndCount(4, "files processed and inserted"));
  }
  
  public void loadEntityMap(Integer projectID) {
    logger.info("  Populating entity map...");
    
    TimeCounter counter = new TimeCounter();
  
    for (MediumEntityDB entity : entityQueries.getMediumExternalByProjectID(projectID)) {
      Ent ent = entityMap.get(entity.getFqn());
      if (ent == null) {
        ent = new Ent(entity.getFqn());
        entityMap.put(entity.getFqn(), ent);
        ent.addPair(entity);
        counter.increment();
      } else {
        logger.severe("    FQN conflict! " + entity.getFqn());
      }
    }
    
    logger.info(counter.reportTimeAndCount(4, "entities loaded"));
  }
  
  protected void insertRemainingEntities(Extracted extracted, Integer projectID) {
    logger.info("  Inserting type entities....");

    TimeCounter counter = new TimeCounter();
    
    entitiesTable.initializeInserter(tempDir);
    
    logger.info("    Processing from local variables / parameters file...");
    for (LocalVariableEX local : extracted.getLocalVariableReader()) {
      // Get the file
      Integer fileID = getFileID(local.getPath(), local);
      
      // Add the entity
      entitiesTable.insertLocalVariable(local, projectID, fileID);
        
      // Resolve the type fqn
      resolveEid(counter, local.getTypeFqn(), projectID, inClause);
      
      counter.increment();
    }
    logger.info(counter.reportTimeAndCount(6, "entities processed"));
    
    counter.lap();
    
    logger.info("    Processing from relations file...");
    for (RelationEX relation : extracted.getRelationReader()) {
      // Resolve the rhs fqn
      resolveEid(counter, relation.getRhs(), projectID, inClause);
    }
    logger.info(counter.reportTimeAndCount(6, "entities processed"));
    
    counter.lap();
    
    logger.info("    Processing from imports file...");
    for (ImportEX imp : extracted.getImportReader()) {
      // Resolve the type fqn
      resolveEid(counter, imp.getImported(), projectID, inClause);
    }
    logger.info(counter.reportTimeAndCount(6, "entities processed"));
    
    counter.lap();
    
    logger.info("    Performing db insert...");
    
    entitiesTable.flushInserts();
    logger.info(counter.reportTimeAndTotalCount(6, "entities inserted"));
    
    logger.info(counter.reportTotalTimeAndCount(4, "entities processed and inserted"));
  }
  
  private void resolveEid(TimeCounter counter, String fqn, Integer projectID, String inClause) {
    if (pendingEntities.contains(fqn)) {
      return;
    }
    // Maybe it's in the map
    if (entityMap.containsKey(fqn)) {
      if (entityMap.get(fqn).resolveDuplicates(projectID)) {
        counter.increment();
      }
      pendingEntities.add(fqn);
      return; 
    }
    
    // If it's a method, skip the type entities
    if (!TypeUtils.isMethod(fqn)) {
      if (TypeUtils.isArray(fqn)) {
        Pair<String, Integer> arrayInfo = TypeUtils.breakArray(fqn);
        
        // Insert the array entity
        entitiesTable.insertArray(fqn, arrayInfo.getSecond(), projectID);
        
        // Add has elements of relation
        resolveEid(counter, arrayInfo.getFirst(), projectID, inClause);
        newTypeRelations.add(RelationEX.getSyntheticRelation(Relation.HAS_ELEMENTS_OF, fqn, arrayInfo.getFirst()));
        
        counter.increment();
        pendingEntities.add(fqn);
        return;
      }
      
      if (TypeUtils.isWildcard(fqn)) {
        // Insert the wildcard entity
        entitiesTable.insert(Entity.WILDCARD, fqn, projectID);
      
        // If it's bounded, add the bound relation
        if (!TypeUtils.isUnboundedWildcard(fqn)) {
          String bound = TypeUtils.getWildcardBound(fqn);
          resolveEid(counter, bound, projectID, inClause);
          if (TypeUtils.isLowerBound(fqn)) {
            newTypeRelations.add(RelationEX.getSyntheticRelation(Relation.HAS_LOWER_BOUND, fqn, bound));
          } else {
            newTypeRelations.add(RelationEX.getSyntheticRelation(Relation.HAS_UPPER_BOUND, fqn, bound));
          }
        }
        
        counter.increment();
        pendingEntities.add(fqn);
        return;
      }
      
      if (TypeUtils.isTypeVariable(fqn)) {
        // Insert the type variable entity
        entitiesTable.insert(Entity.TYPE_VARIABLE, fqn, projectID);
        
        // Insert the bound relations
        for (String bound : TypeUtils.breakTypeVariable(fqn)) {
          resolveEid(counter, bound, projectID, inClause);
          newTypeRelations.add(RelationEX.getSyntheticRelation(Relation.HAS_UPPER_BOUND, fqn, bound));
        }
        
        counter.increment();
        pendingEntities.add(fqn);
        return;
      }
      
      if (TypeUtils.isParametrizedType(fqn)) {
        // Insert the parametrized type entity
        entitiesTable.insert(Entity.PARAMETERIZED_TYPE, fqn, projectID);
        
        // Add the has base type relation
        String baseType = TypeUtils.getBaseType(fqn);
        resolveEid(counter, baseType, projectID, inClause);
        newTypeRelations.add(RelationEX.getSyntheticRelation(Relation.HAS_BASE_TYPE, fqn, baseType));
        
        // Insert the type arguments
        for (String arg : TypeUtils.breakParametrizedType(fqn)) {
          resolveEid(counter, arg, projectID, inClause);
          newTypeRelations.add(RelationEX.getSyntheticRelation(Relation.HAS_TYPE_ARGUMENT, fqn, arg));
        }
        
        counter.increment();
        pendingEntities.add(fqn);
        return; 
      }
    }

    // Some external reference?
    Collection<SmallEntityDB> entities = entityQueries.getSmallByFqn(fqn, inClause);
    if (!entities.isEmpty()) {
      Ent result = new Ent(fqn);
      for (SmallEntityDB entity : entities) {
        result.addPair(entity);
      }
      result.resolveDuplicates(projectID);
      entityMap.put(fqn, result);
      counter.increment();
      pendingEntities.add(fqn);
      return;
    }
    
    // Give up
    // Check if it's an already known unknown
    if (!unknowns.contains(fqn)) {
      unknowns.add(entitiesTable, fqn);
      counter.increment();
    }
    pendingEntities.add(fqn);
  }
  
  protected void loadRemainingEntityMap(Integer projectID) {
    logger.info("  Updating entity map...");
    
    TimeCounter counter = new TimeCounter();

    relationsTable.initializeInserter(tempDir);
    
    logger.info("    Loading project entities...");
    for (MediumEntityDB entity : entityQueries.getMediumSyntheticByProjectID(projectID)) {
      if (!entity.getType().isDuplicate()) {
        Ent ent = entityMap.get(entity.getFqn());
        if (ent == null) {
          ent = new Ent(entity.getFqn());
          entityMap.put(entity.getFqn(), ent);
          ent.addPair(entity);
          counter.increment();
        } else {
          logger.severe("    FQN conflict! " + entity.getFqn());
        }
      } else {
        if (entityMap.containsKey(entity.getFqn())) {
          entityMap.get(entity.getFqn()).updateDuplicate(entity.getEntityID(), projectID);
        } else {
          logger.severe("Missing fqn for duplicate " + entity.getFqn());
        }
      }
    }
    logger.info(counter.reportTimeAndCount(6, "synthetic entities loaded"));
    
    counter.reset();
    
    logger.info("    Performing db insert on duplicate relations...");
    relationsTable.flushInserts();
    logger.info(counter.reportTime(6, "Db insert performed"));
    
    counter.reset();
     
    pendingEntities.clear();
    
    logger.info(counter.reportTotalTime(4, "Entity map updated"));
  }
  
  protected void insertRelations(Extracted extracted, Integer projectID) {
    logger.info("  Inserting relations...");
    
    TimeCounter counter = new TimeCounter();
    
    relationsTable.initializeInserter(tempDir);
    
    logger.info("    Processing type relations...");
    for (RelationEX relation : newTypeRelations) {
      SmallEntityDB lhs = getEid(relation.getLhs(), projectID);
      SmallEntityDB rhs = getEid(relation.getRhs(), projectID);
      
      if (lhs != null && rhs != null) {
        relationsTable.insert(relation.getType(), classifier.getRelationClass(lhs, rhs), lhs.getEntityID(), rhs.getEntityID(), projectID);
        counter.increment();
      }
    }
    newTypeRelations.clear();
    
    logger.info(counter.reportTimeAndCount(6, "relations processed"));
      
    counter.lap();
      
    logger.info("      Processing relations file...");
    for (RelationEX relation : extracted.getRelationReader()) {
      Integer fileID = getFileID(relation.getPath(), relation);
      
      SmallEntityDB lhs = getEid(relation.getLhs(), projectID);
      SmallEntityDB rhs = getEid(relation.getRhs(), projectID);
      
      if (lhs != null && rhs != null) {
        if (fileID == null) {
          relationsTable.insert(relation.getType(), classifier.getRelationClass(lhs, rhs), lhs.getEntityID(), rhs.getEntityID(), projectID);
        } else {
          relationsTable.insert(relation.getType(), classifier.getRelationClass(lhs, rhs), lhs.getEntityID(), rhs.getEntityID(), projectID, fileID, relation.getStartPosition(), relation.getLength());
        }
        counter.increment();
      }
    }
    logger.info(counter.reportTimeAndCount(6, "relations processed"));
    
    logger.info("    Processing local variables / parameters file...");
    Iterator<LocalVariableEX> iter = extracted.getLocalVariableReader().iterator();
    for (SmallEntityDB entity : entityQueries.getMediumLocalByProjectID(projectID)) {
      if (iter.hasNext()) {
        LocalVariableEX local = iter.next();
        
        Integer fileID = getFileID(local.getPath(), local);
        
        // Add the holds relation
        SmallEntityDB type = getEid(local.getTypeFqn(), projectID);
        if (type != null) {
          if (fileID == null) {
            relationsTable.insert(Relation.HOLDS, classifier.getRelationClass(entity, type), entity.getEntityID(), type.getEntityID(), projectID);
          } else {
            relationsTable.insert(Relation.HOLDS, classifier.getRelationClass(entity, type), entity.getEntityID(), type.getEntityID(), projectID, fileID, local.getStartPos(), local.getLength());
          }
          counter.increment();
        }
        
        // Add the inside relation
        SmallEntityDB parent = getEid(local.getParent(), projectID);
        if (parent != null) {
          relationsTable.insert(Relation.INSIDE, classifier.getRelationClass(entity, parent), entity.getEntityID(), parent.getEntityID(), projectID, fileID, null, null);
        }
        
        counter.increment();
      } else {
        logger.log(Level.SEVERE, "Missing db local variable for " + entity);
      }
    }
    
    logger.info(counter.reportTimeAndCount(6, "relations processed"));
    
    logger.info("    Performing db insert...");
    relationsTable.flushInserts();
    logger.info(counter.reportTimeAndTotalCount(6, "relations inserted"));
  }
  
  protected void insertImports(Extracted extracted, Integer projectID) {
    logger.info("  Inserting imports...");
    
    TimeCounter counter = new TimeCounter();
    
    importsTable.initializeInserter(tempDir);
    
    logger.info("    Processing imports file...");
    for (ImportEX imp : extracted.getImportReader()) {
      Integer fileID = getFileID(imp.getPath(), imp);
      
      if (fileID != null) {
        // Look up the imported entity
        SmallEntityDB imported = getEid(imp.getImported(), projectID);
        
        // Add the import
        if (imported != null) {
          importsTable.insert(imp.isStatic(), imp.isOnDemand(), imported.getEntityID(), projectID, fileID, imp.getOffset(), imp.getLength());
          counter.increment();
        }
      }
    }
    logger.info(counter.reportTimeAndCount(6, "imports processed"));
    
    counter.lap();
    
    logger.info("    Performing db insert...");
    importsTable.flushInserts();
    logger.info(counter.reportTimeAndTotalCount(6, "imports inserted"));
  }
  
  protected void insertComments(Extracted extracted, Integer projectID) {
    logger.info("  Inserting comments...");
    
    TimeCounter counter = new TimeCounter();

    commentsTable.initializeInserter(tempDir);
    
    logger.info("    Processing comments file...");
    for (CommentEX comment : extracted.getCommentReader()) {
      Integer fileID = getFileID(comment.getPath(), comment);
      
      if (fileID != null) {
        if (comment.getType() == Comment.JAVADOC) {
          // Look up the entity
          SmallEntityDB commented = getEid(comment.getFqn(), projectID);
          
          // Add the comment
          if (commented != null) {
            commentsTable.insertJavadoc(commented.getEntityID(), projectID, fileID, comment.getOffset(), comment.getLength());
            counter.increment();
          }
        } else if (comment.getType() == Comment.UJAVADOC) {
          // Add the comment
          commentsTable.insertUnassociatedJavadoc(projectID, fileID, comment.getOffset(), comment.getLength());
          counter.increment();
        } else {
          commentsTable.insertComment(comment.getType(), projectID, fileID, comment.getOffset(), comment.getLength());
          counter.increment();
        }
      }
    }
    logger.info(counter.reportTimeAndCount(6, "comments processed"));
    
    
    logger.info("    Performing db insert...");
    commentsTable.flushInserts();
    logger.info(counter.reportTimeAndTotalCount(6, "comments inserted"));
  }
  
  private Integer getFileID(String path, ModelEX model) {
    if (path == null) {
      return null;
    } else {
      Integer fileID = fileMap.get(path);
      if (fileID == null) {
        logger.log(Level.SEVERE, "Unknown file: " + path + " for " + model);
      }
      return fileID;
    }
  }
  
  public SmallEntityDB getEid(String fqn, Integer projectID) {
    Ent ent = entityMap.get(fqn);
    if (ent == null) {
      SmallEntityDB entity = unknowns.getUnknown(fqn);
      if (entity == null) {
        logger.severe("Unknown entity: " + fqn);
        return null;
      } else {
        return entity;
      }
    } else {
      return ent.getEntity(projectID);
    }
  }
  
  protected void buildInClause(Collection<Integer> projectIDs, Extracted extracted) {
    for (UsedJarEX usedJar : extracted.getUsedJarReader()) {
      projectIDs.add(projectQueries.getProjectIDByHash(usedJar.getHash()));
    }
    for (FileEX file : extracted.getFileReader()) {
      if (file.getType() == File.JAR) {
        projectIDs.add(projectQueries.getProjectIDByHash(file.getHash()));
      }
    }
    buildInClause(projectIDs);
  }
  
  protected void buildInClause(Collection<Integer> projectIDs) {
    StringBuilder builder = new StringBuilder("(");
    for (Integer projectID : projectIDs) {
      builder.append(projectID).append(',');
    }
    builder.setCharAt(builder.length() - 1, ')');
    inClause = builder.toString();
  }
  
  private class Ent {
    private String fqn;
    
    private SmallEntityDB main = null;
    private Map<Integer, SmallEntityDB> entities = null;

    public Ent(String fqn) {
      this.fqn = fqn;
    }
    
    public void addPair(SmallEntityDB entity) {
      if (entities == null && main == null) {
        main = entity;
      } else {
        if (entities == null) {
          entities = Helper.newHashMap();
          main = null;
        }
        entities.put(entity.getProjectID(), entity);
      }
    }
    
    public boolean resolveDuplicates(Integer projectID) {
      if (entities != null && !entities.containsKey(projectID)) {
        entitiesTable.insert(Entity.DUPLICATE, fqn, projectID);
        return true;
      } else {
        return false;
      }
    }
    
    public void updateDuplicate(Integer eid, Integer projectID) {
      for (SmallEntityDB entity : entities.values()) {
        if (!entity.getType().isDuplicate()) {
          relationsTable.insert(Relation.MATCHES, RelationClass.NOT_APPLICABLE, eid, entity.getEntityID(), projectID);
        }
      }
      entities.put(projectID, new SmallEntityDB(eid, Entity.DUPLICATE, projectID));
    }
    
    public SmallEntityDB getEntity(Integer projectID) {
      if (entities == null) {
        return main;
      } else {
        return entities.get(projectID);
      }
    }
  }
}
