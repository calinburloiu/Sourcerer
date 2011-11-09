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
package edu.uci.ics.sourcerer.tools.java.utilization.identifier;

import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.sourcerer.tools.java.utilization.model.FqnFragment;
import edu.uci.ics.sourcerer.tools.java.utilization.model.JarSet;
import edu.uci.ics.sourcerer.util.Averager;
import edu.uci.ics.sourcerer.util.io.arguments.Argument;
import edu.uci.ics.sourcerer.util.io.arguments.DoubleArgument;

/**
 * @author Joel Ossher (jossher@uci.edu)
 */
public class Library {
  public static final Argument<Double> COMPATIBILITY_THRESHOLD = new DoubleArgument("compatibility-threshold", 1., "").permit();
  private JarSet jars;
  private final Set<FqnFragment> fqns;
  
  Library() {
    this.fqns = new HashSet<>();
    jars = JarSet.makeEmpty();
  }
  
  void addFqn(FqnFragment fqn) {
    fqns.add(fqn);
    jars = jars.merge(fqn.getJars());
  }
  
  public Set<FqnFragment> getFqns() {
    return fqns;
  }
  
  public JarSet getJars() {
    return jars;
  }
  
  public boolean isCompatible(Library other) {
    // Do a pairwise comparison of every FQN. Calculate the conditional
    // probability of each FQN in B appearing given each FQN in A and average.
    // Then compute the reverse. Both values must be above the threshold.
    double threshold = COMPATIBILITY_THRESHOLD.getValue();
    Averager<Double> otherGivenThis = new Averager<>();
    Averager<Double> thisGivenOther = new Averager<>();
    
    for (FqnFragment fqn : fqns) {
      for (FqnFragment otherFqn : other.fqns) {
        JarSet fqnJars = fqn.getJars();
        JarSet otherFqnJars = otherFqn.getJars();
        // Conditional probability of other given this
        // # shared jars / total jars in this
        otherGivenThis.addValue((double) fqnJars.getIntersectionSize(otherFqnJars) / fqnJars.size());
        // Conditional probabilty for this given other
        // # shared jars / total jars in other
        thisGivenOther.addValue((double) otherFqnJars.getIntersectionSize(fqnJars) / otherFqnJars.size());
      }
    }
    return otherGivenThis.getMean() >= threshold && thisGivenOther.getMean() >= threshold;
  }
  
  public String toString() {
    return fqns.toString();
  }
}
