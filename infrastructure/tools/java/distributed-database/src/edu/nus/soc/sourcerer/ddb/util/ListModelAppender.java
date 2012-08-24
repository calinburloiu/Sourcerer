package edu.nus.soc.sourcerer.ddb.util;

import java.util.LinkedList;
import java.util.List;

public class ListModelAppender<T> implements ModelAppender<T> {
  
  protected List<T> list = new LinkedList<T>();
  int count = 0;
  int limit = Integer.MAX_VALUE;
  
  public ListModelAppender() {
    super();
  }

  public ListModelAppender(int limit) {
    super();
    this.limit = limit;
  }

  @Override
  public boolean add(T model) {
    if (model == null)
      return true;
    if (count >= limit)
      return false;
    
    list.add(model);
    count++;
    return true;
  }

  public List<T> getList() {
    return list;
  }
}
