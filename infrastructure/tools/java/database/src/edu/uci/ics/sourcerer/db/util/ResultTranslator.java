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
package edu.uci.ics.sourcerer.db.util;

import edu.uci.ics.sourcerer.db.util.columns.Column;

/**
 * @author Joel Ossher (jossher@uci.edu)
 */
public abstract class ResultTranslator<T> extends BasicResultTranslator<T> {
  private final String table;
  private final String select;
  
  public ResultTranslator(String table, Column<?> ... columns) {
    this.table = table;
    StringBuilder builder = new StringBuilder();
    for (Column<?> column : columns) {
      builder.append(column.getQualifiedName()).append(',');
    }
    builder.deleteCharAt(builder.length() - 1);
    select = builder.toString();
  }
  
  public final String getTable() {
    return table;
  }
  
  public final String getSelect() {
    return select;
  }
}