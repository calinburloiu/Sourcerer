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

import java.util.Collection;

import edu.uci.ics.sourcerer.util.Helper;
import edu.uci.ics.sourcerer.util.Pair;

/**
 * @author Joel Ossher (jossher@uci.edu)
 */
public final class TypeUtils {
  private TypeUtils() {}
  
  public static boolean isMethod(String fqn) {
    // ...(...)
    return fqn.contains("(") && fqn.endsWith(")");
  }
  
  public static boolean isArray(String fqn) {
    // ...[]
    return fqn.endsWith("[]");
  }
  
  public static Pair<String, Integer> breakArray(String fqn) {
    int arrIndex = fqn.indexOf("[]");
    String elementFqn = fqn.substring(0, arrIndex);
    int dimensions = (fqn.length() - arrIndex) / 2;
    return new Pair<String, Integer>(elementFqn, dimensions);
  }
  
  public static boolean isWildcard(String fqn) {
    // <?...>
    return fqn.startsWith("<?") && fqn.endsWith(">");
  }
  
  public static boolean isUnboundedWildcard(String fqn) {
    // <?>
    return "<?>".equals(fqn);
  }
  
  public static boolean isLowerBound(String fqn) {
    // <?-...>
    return fqn.charAt(2) == '-';
  }
  
  public static String getWildcardBound(String fqn) {
    // <?+...>
    return fqn.substring(3, fqn.length() - 1);
  }
  
  public static boolean isTypeVariable(String fqn) {
    // <...>
    return fqn.startsWith("<") && fqn.endsWith(">");
  }
  
  public static Collection<String> breakTypeVariable(String typeVariable) {
    Collection<String> parts = Helper.newLinkedList();
    
    StringBuilder builder = new StringBuilder();
    int depth = 0;
    boolean afterPlus = false;
    for (char c : typeVariable.toCharArray()) {
      if (depth == 0) {
        if (c == '<') {
          depth++;
        } else {
          throw new IllegalArgumentException(typeVariable + " is not a valid type variable");
        }
      } else if (depth == 1) {
        if (afterPlus) {
          if (c == '&') {
            parts.add(builder.toString());
            builder.setLength(0);
          } else if (c == '>') {
            depth--;
            parts.add(builder.toString());
          } else if (c == '<') {
            depth++;
            builder.append(c);
          } else {
            builder.append(c);
          }
        } else if (c == '+') {
          afterPlus = true;
        }
      } else {
        if (c == '<') {
          depth++;
        } else if (c == '>') {
          depth--;
        }
        builder.append(c);
      }
      
    }
   
    return parts;
  }
  
  public static boolean isParametrizedType(String fqn) {
    int baseIndex = fqn.indexOf('<');
    return baseIndex > 0 && fqn.indexOf('>') > baseIndex;
  }
  
  public static String getBaseType(String parametrizedType) {
    StringBuilder builder = new StringBuilder();
    int depth = 0;
    for (char c : parametrizedType.toCharArray()) {
      if (c == '<') {
        depth++;
      } else if (c == '>') {
        depth--;
      } else if (depth == 0) {
        builder.append(c);
      }
    }
    return builder.toString();
  }
  
  public static Collection<String> breakParametrizedType(String fqn) {
    Collection<String> parts = Helper.newLinkedList();
    
    StringBuilder builder = new StringBuilder();
    int depth = 0;
    for (char c : fqn.toCharArray()) {
      if (depth == 0) {
        if (c == '<') {
          depth++;
        }
      } else if (depth == 1) {
        if (c == ',') {
          parts.add(builder.toString());
          builder.setLength(0);
        } else if (c == '>') {
          depth--;
          parts.add(builder.toString());
        } else if (c == '<') {
          depth++;
          builder.append(c);
        } else {
          builder.append(c);
        }
      } else {
        if (c == '<') {
          depth++;
        } else if (c == '>') {
          depth--;
        }
        builder.append(c);
      }
    }
    
    return parts;
  }
}
