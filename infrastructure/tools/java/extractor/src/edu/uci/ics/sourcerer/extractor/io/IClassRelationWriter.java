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
package edu.uci.ics.sourcerer.extractor.io;

/**
 * @author Joel Ossher (jossher@uci.edu)
 */
public interface IClassRelationWriter extends IExtractorWriter {
  public void writeInside(String innerFqn, String outerFqn, String path);

  public void writeExtends(String subTypeFqn, String superTypeFqn, String path);

  public void writeImplements(String subTypeFqn, String superTypeFqn, String path);

  public void writeHolds(String fqn, String type, String path);
 
  public void writeReturns(String fqn, String returnType, String path);
  
  public void writeThrows(String fqn, String exceptionType, String path);
  
  public void writeParametrizedBy(String fqn, String typeVariable, int position, String path);
}
