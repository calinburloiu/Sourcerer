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
  UNKNOWN           ((byte)0x00),
  INSIDE            ((byte)0x01),
  EXTENDS           ((byte)0x02),
  IMPLEMENTS        ((byte)0x03),
  HOLDS             ((byte)0x04),
  RETURNS           ((byte)0x05),
  READS             ((byte)0x06),
  WRITES            ((byte)0x07),
  CALLS             ((byte)0x08),
  USES              ((byte)0x09),
  INSTANTIATES      ((byte)0x0a),
  THROWS            ((byte)0x0b),
  CASTS             ((byte)0x0c),
  CHECKS            ((byte)0x0d),
  ANNOTATED_BY      ((byte)0x0e),
  HAS_ELEMENTS_OF   ((byte)0x0f),
  PARAMETRIZED_BY   ((byte)0x10),
  HAS_BASE_TYPE     ((byte)0x11),
  HAS_TYPE_ARGUMENT ((byte)0x12),
  HAS_UPPER_BOUND   ((byte)0x13),
  HAS_LOWER_BOUND   ((byte)0x14),
  OVERRIDES         ((byte)0x15),
  MATCHES           ((byte)0x16),
  ;

  protected byte value;
  
  public static final byte MASK = ((byte)0x1F);
  
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