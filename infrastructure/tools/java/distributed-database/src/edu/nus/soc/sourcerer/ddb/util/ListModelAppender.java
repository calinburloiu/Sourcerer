package edu.nus.soc.sourcerer.ddb.util;

import java.util.LinkedList;
import java.util.List;

public class ListModelAppender<T> implements ModelAppender<T> {
  protected List<T> list = new LinkedList<T>();
  
  @Override
  public void add(T model) {
    list.add(model);
  }

  public List<T> getList() {
    return list;
  }
}
