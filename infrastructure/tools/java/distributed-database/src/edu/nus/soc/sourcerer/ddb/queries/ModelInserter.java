package edu.nus.soc.sourcerer.ddb.queries;

import java.util.Collection;

import edu.nus.soc.sourcerer.ddb.HBaseException;
import edu.nus.soc.sourcerer.model.ddb.Model;

public interface ModelInserter<E extends Model> {
  
  public void insertModels(Collection<E> models)
      throws HBaseException;

}
