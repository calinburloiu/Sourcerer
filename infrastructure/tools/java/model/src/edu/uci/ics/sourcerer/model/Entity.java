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

import edu.nus.soc.sourcerer.util.EnumWithValue;

/**
 * @author Joel Ossher (jossher@uci.edu)
 */
public enum Entity implements EnumWithValue<Byte> {
    UNKNOWN             ((byte)0x00),
    PACKAGE             ((byte)0x01),
    CLASS               ((byte)0x02),
    INTERFACE           ((byte)0x03),
    ENUM                ((byte)0x04),
    ANNOTATION          ((byte)0x05),
    INITIALIZER         ((byte)0x06),
    FIELD               ((byte)0x07),
    ENUM_CONSTANT       ((byte)0x08),
    CONSTRUCTOR         ((byte)0x09),
    METHOD              ((byte)0x0a),
    ANNOTATION_ELEMENT  ((byte)0x0b),
    PARAMETER           ((byte)0x0c),
    LOCAL_VARIABLE      ((byte)0x0d),
    PRIMITIVE           ((byte)0x0e),
    ARRAY               ((byte)0x0f),
    TYPE_VARIABLE       ((byte)0x10),
    WILDCARD            ((byte)0x11),
    PARAMETERIZED_TYPE  ((byte)0x12),
    DUPLICATE           ((byte)0x13)
    ;

    protected byte value;
    
    private Entity(byte value) {
      this.value = value;
    }
    
    public Byte getValue() {
      return value;
    }
    
    public boolean isDeclaredType() {
      return this == CLASS || this == INTERFACE || this == ENUM || this == ANNOTATION;
    }
    
    public boolean isInternalMeaningful() {
      return !(this == PACKAGE || this == ARRAY || this == TYPE_VARIABLE || this == WILDCARD || this == PARAMETERIZED_TYPE || this == UNKNOWN);
    }

    public boolean isPackage() {
      return this == PACKAGE;
    }

    public boolean isAnnotation() {
      return this == ANNOTATION;
    }

    public boolean isInitializer() {
      return this == INITIALIZER;
    }
    
    public boolean isInterface() {
      return this == INTERFACE;
    }

    public boolean isEnum() {
      return this == ENUM;
    }

    public boolean isClass() {
      return this == CLASS;
    }

    public boolean isArray() {
      return this == ARRAY;
    }

    public boolean isParametrizedType() {
      return this == PARAMETERIZED_TYPE;
    }

    public boolean isCallableType() {
      return this == METHOD || this == CONSTRUCTOR;
    }

    public boolean isMethod() {
      return this == METHOD;
    }

    public boolean isConstructor() {
      return this == CONSTRUCTOR;
    }

    public boolean isUnknown() {
      return this == UNKNOWN;
    }

    public boolean isFieldImport() {
      return this == FIELD || this == ENUM_CONSTANT;
    }

    public boolean isPrimitive() {
      return this == PRIMITIVE;
    }

    public boolean isImportable() {
      return this != PRIMITIVE && this != UNKNOWN;
    }
    
    public boolean isDuplicate() {
      return this == DUPLICATE;
    }
    
    public static Entity parse(String name) {
      if (name == null) {
        return null;
      } else {
        for (Entity entity : values()) {
          if (entity.name().equals(name)) {
            return entity;
          }
        }
        return null;
      }
    }
    
    public static String getPossiblePackage(String fqn) {
      // unmethodify it
      int index = fqn.indexOf('(');
      if (index >= 0) {
        fqn = fqn.substring(0, index);
      }
      // unparametrized type it
      index = fqn.indexOf('<');
      if (index >= 0) {
        fqn = fqn.substring(0, index);
      }
      // get the potential package name
      index = fqn.lastIndexOf('.');
      if (index == -1) {
        return fqn;
      } else {
        return fqn.substring(0, index);
      }
    }
    

  }