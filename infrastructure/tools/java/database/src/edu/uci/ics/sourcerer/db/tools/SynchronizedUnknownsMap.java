/* 
 * Sourcerer: an infrastructure for large-scale source code analysis.
 * Copyright (C) by contributors. See CONTRIBUTORS.txt for full list.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package edu.uci.ics.sourcerer.db.tools;

import static edu.uci.ics.sourcerer.util.io.Logging.logger;

import java.util.Map;
import java.util.logging.Level;

import edu.uci.ics.sourcerer.db.queries.DatabaseAccessor;
import edu.uci.ics.sourcerer.db.schema.EntitiesTable;
import edu.uci.ics.sourcerer.db.util.DatabaseConnection;
import edu.uci.ics.sourcerer.model.Entity;
import edu.uci.ics.sourcerer.model.db.MediumEntityDB;
import edu.uci.ics.sourcerer.model.db.SmallEntityDB;
import edu.uci.ics.sourcerer.util.Helper;

/**
 * @author Joel Ossher (jossher@uci.edu)
 */
public class SynchronizedUnknownsMap extends DatabaseAccessor {
  private Integer unknownsProject;
  private volatile Map<String, Integer> unknowns;
  
  public SynchronizedUnknownsMap(DatabaseConnection connection) {
    super(connection);
    
    unknownsProject = projectQueries.getUnknownsProjectID();
    
    unknowns = Helper.newHashMap();
    for (MediumEntityDB entity : entityQueries.getMediumByProjectIDStreamed(unknownsProject, Entity.UNKNOWN)) {
      unknowns.put(entity.getFqn(), entity.getEntityID());
    }
    close();
    connection.close();
  }
  
  protected synchronized void add(EntitiesTable entitiesTable, String fqn) {
    if (!contains(fqn)) {
      Integer eid = entitiesTable.forceInsertUnknown(fqn, unknownsProject);
      if (eid == null) {
        logger.log(Level.SEVERE, "Missing eid for unknown: " + fqn);
      } else {
        unknowns.put(fqn, eid);
      }
    }
  }
  
  protected synchronized boolean contains(String fqn) {
    return unknowns.containsKey(fqn);
  }
  
  protected synchronized SmallEntityDB getUnknown(String fqn) {
    Integer eid = unknowns.get(fqn);
    if (eid == null) {
      return null;
    } else {
      return new SmallEntityDB(unknowns.get(fqn), Entity.UNKNOWN, unknownsProject);
    }
  }
}
