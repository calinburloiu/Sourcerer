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
package edu.uci.ics.sourcerer.model;

/**
 * @author Joel Ossher (jossher@uci.edu)
 */
public enum Relation {
  INSIDE            ((byte)0x00),
  EXTENDS           ((byte)0x01),
  IMPLEMENTS        ((byte)0x02),
  HOLDS             ((byte)0x03),
  RETURNS           ((byte)0x04),
  READS             ((byte)0x05),
  WRITES            ((byte)0x06),
  CALLS             ((byte)0x07),
  USES              ((byte)0x08),
  INSTANTIATES      ((byte)0x09),
  THROWS            ((byte)0x0a),
  CASTS             ((byte)0x0b),
  CHECKS            ((byte)0x0c),
  ANNOTATED_BY      ((byte)0x0d),
  HAS_ELEMENTS_OF   ((byte)0x0e),
  PARAMETRIZED_BY   ((byte)0x0f),
  HAS_BASE_TYPE     ((byte)0x10),
  HAS_TYPE_ARGUMENT ((byte)0x11),
  HAS_UPPER_BOUND   ((byte)0x12),
  HAS_LOWER_BOUND   ((byte)0x13),
  OVERRIDES         ((byte)0x14),
  MATCHES           ((byte)0x15),
  ;

  protected byte value;
  
  private Relation(byte value) {
    this.value = value;
  }
  
  public byte getValue() {
    return value;
  }
  
  public static Relation parse(String name) {
    if (name == null) {
      return null;
    } else {
      for (Relation relation : values()) {
        if (relation.name().equals(name)) {
          return relation;
        }
      }
      return null;
    }
  }
}