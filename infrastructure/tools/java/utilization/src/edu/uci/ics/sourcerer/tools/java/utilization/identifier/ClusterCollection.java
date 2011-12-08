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

import static edu.uci.ics.sourcerer.util.io.Logging.logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import edu.uci.ics.sourcerer.tools.java.repo.model.JarProperties;
import edu.uci.ics.sourcerer.tools.java.utilization.model.FqnFragment;
import edu.uci.ics.sourcerer.tools.java.utilization.model.Jar;
import edu.uci.ics.sourcerer.util.io.IOUtils;
import edu.uci.ics.sourcerer.util.io.TaskProgressLogger;
import edu.uci.ics.sourcerer.util.io.arguments.Arguments;

/**
 * @author Joel Ossher (jossher@uci.edu)
 */
public class ClusterCollection implements Iterable<Cluster> {
  private final Collection<Cluster> clusters;
  
  ClusterCollection() {
    clusters = new ArrayList<>();
  }
  
  void addCluster(Cluster library) {
    clusters.add(library);
  }
  
  public Collection<Cluster> getClusters() {
    return clusters;
  }

  @Override
  public Iterator<Cluster> iterator() {
    return clusters.iterator();
  }

  public int size() {
    return clusters.size();
  }
  
//  public void printStatistics(TaskProgressLogger task) {
//    task.start("Printing library statistics");
//    task.report(libraries.size() + " libraries identified");
//    int trivial = 0;
//    TreeSet<Library> nonTrivial = new TreeSet<>(new Comparator<Library>() {
//      @Override
//      public int compare(Library o1, Library o2) {
//        int cmp = Integer.compare(o1.getJars().size(), o2.getJars().size());
//        if (cmp == 0) {
//          return Integer.compare(o1.hashCode(), o2.hashCode());
//        } else {
//          return cmp;
//        }
//      }});
//    for (Library library : libraries) {
//      if (library.getJars().size() > 1) {
//        nonTrivial.add(library);
//      } else {
//        trivial++;
//      }
//    }
//    task.report(trivial + " unique libraries");
//    task.report(nonTrivial.size() + " compound libraries");
//    task.start("Examining compound libraries");
//    while (!nonTrivial.isEmpty()) {
//      Library biggest = nonTrivial.pollLast();
//      task.start("Listing FQNs for library found in " + biggest.getJars().size() + " jars");
//      for (FqnFragment fqn : biggest.getFqns()) {
//        task.report(fqn.getFqn());
//      }
//      task.finish();
//    }
//    task.finish();
//    task.finish();
//  }
  
  public void printStatistics(TaskProgressLogger task, String jarFileName, String clusterFileName) {
    NumberFormat format = NumberFormat.getPercentInstance();
    format.setMinimumFractionDigits(2);
    format.setMaximumFractionDigits(2);

    task.start("Printing jar statistics");
    try (BufferedWriter bw = IOUtils.makeBufferedWriter(new File(Arguments.OUTPUT.getValue(), jarFileName))) {
      Multimap<Jar, Cluster> clusterMap = HashMultimap.create();
      int trivialCluster = 0;
      for (Cluster cluster : clusters) {
        for (Jar jar : cluster.getJars()) {
          clusterMap.put(jar, cluster);
        }
        if (cluster.getJars().size() == 1) {
          trivialCluster++;
        }
      }
      int trivialJar = 0;
      for (Jar jar : clusterMap.keySet()) {
        Collection<Cluster> clusters = clusterMap.get(jar);
        if (clusters.size() == 1) {
          trivialJar++;
        }
      }
      bw.write(clusterMap.keySet().size() + " jars");
      bw.newLine();
      bw.write(clusters.size() + " clusters");
      bw.newLine();
      bw.write(trivialJar + " jars covered by single cluster");
      bw.newLine();
      bw.write(trivialCluster + " clusters matching a single jar");
      bw.newLine();
      bw.write((clusterMap.keySet().size() - trivialJar) + " jars covered by multiple clusters");
      bw.newLine();
      bw.write((clusters.size() - trivialCluster) + " clustered matching multiple jars");
      bw.newLine();
      
      for (Jar jar : clusterMap.keySet()) {
        Collection<Cluster> clusters = clusterMap.get(jar);
        if (clusters.size() > 1) {
          HashSet<FqnFragment> fqns = new HashSet<>(jar.getFqns());
          bw.write(jar.getJar().getProperties().NAME.getValue() + " fragmented into " + clusters.size() + " clusters");
          bw.newLine();
          Set<Jar> otherJars = new HashSet<>();
          for (FqnFragment fqn : jar.getFqns()) {
            for (Jar otherJar : fqn.getVersions().getJars()) {
              otherJars.add(otherJar);
            }
          }
          bw.write(" FQNs from this jar appear in " + (otherJars.size() - 1) + " other jars");
          bw.newLine();
          bw.write(" Listing jars with overlap");
          bw.newLine();
          int c = 1;
          for (Jar otherJar : otherJars) {
            JarProperties props = otherJar.getJar().getProperties();
            bw.write("  " + c++ + ": " + props.NAME.getValue() + ": " + props.HASH.getValue());
            if (otherJar == jar) {
              bw.write(" <--");
            }
            bw.newLine();
          }
          
          for (int i = 1, max = otherJars.size(); i <= max; i++) {
            bw.write(Integer.toString(i % 10));
          }
          bw.newLine();
          int clusterCount = 0;
          for (Cluster lib : clusters) {
            for (int i = 0; i < c; i++)
              bw.write(" ");
            bw.write(" Cluster " + ++clusterCount + ", from " + lib.getJars().size() + " jars");
            bw.newLine();
            int skipped = 0;
            for (FqnFragment fqn : lib.getFqns()) {
              if (fqns.contains(fqn)) {
                for (Jar otherJar : otherJars) {
                  if (fqn.getVersions().getJars().contains(otherJar)) {
                    bw.write("*");
                  } else {
                    bw.write(" ");
                  }
                }
                bw.write(" " + fqn.getFqn());
                bw.newLine();
              } else {
                skipped++;
              }
            }
            for (int i = 0; i < c; i++)
              bw.write(" ");
            if (skipped > 0)
              bw.write(" " + skipped + " FQNS in cluster not in this jar");
            bw.newLine();
          }
        }
      }
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error printing statistics", e);
    }
    task.finish();
    
    task.start("Printing cluster statistics");
    try (BufferedWriter bw = IOUtils.makeBufferedWriter(new File(Arguments.OUTPUT.getValue(), clusterFileName))) {
      TreeSet<Cluster> sortedClusters = new TreeSet<>(new Comparator<Cluster>() {
        @Override
        public int compare(Cluster o1, Cluster o2) {
          int cmp = Integer.compare(o1.getFqns().size(), o2.getFqns().size());
          if (cmp == 0) {
            return Integer.compare(o1.hashCode(), o2.hashCode());
          } else {
            return cmp;
          }
        }});
      for (Cluster cluster : clusters) {
        if (cluster.getJars().size() > 1) {
          sortedClusters.add(cluster);
        }
      }
      
      bw.write(clusters.size() + " clusters");
      bw.newLine();
      bw.write((clusters.size() - sortedClusters.size()) + " clusters matching a single jar");
      bw.newLine();
      bw.write(sortedClusters.size() + " clustered matching multiple jars");
      bw.newLine();
//    
//    try (BufferedWriter bw = IOUtils.makeBufferedWriter(new File(Arguments.OUTPUT.getValue(), name))) {
//      bw.write(libraries.size() + " libraries identified");
//      bw.newLine();
//      bw.write(clusters.size() + " clusters identified");
//      bw.newLine();
//      
//      int trivial = 0;
//      for (Iterator<TreeSet<Library>> iter = clusters.iterator(); iter.hasNext();) {
//        Set<Library> cluster = iter.next();
//        if (cluster.size() == 1) {
//          trivial++;
//          iter.remove();
//        }
//      }
//      
//      bw.write(trivial + " trivial clusters");
//      bw.newLine();
//      bw.write(clusters.size() + " non-trivial clusters");
//      bw.newLine();
//
//      for (TreeSet<Library> cluster : clusters) {
//        // Count the number of jars in this cluster
//        Set<Jar> jars = new HashSet<>();
//        for (Library lib : cluster) {
//          for (Jar jar : lib.getJars()) {
//            jars.add(jar);
//          }
//        }
//        bw.write("Cluster of " + cluster.size() + " libraries");
//        bw.newLine();
//        bw.write("Cluster spans " + jars.size() + " jars");
//        bw.newLine();
//        int i = 0;
//        while (!cluster.isEmpty()) {
//          Library smallest = cluster.pollFirst();
//          bw.write("  Lib " + ++i);
//          bw.newLine();
//          for (FqnFragment fqn : smallest.getFqns()) {
//            bw.write("    " + fqn.getFqn());
//            bw.newLine();
//          }
//        }
//      }
//      bw.write(trivial + " trivial libraries");
//      bw.newLine();
//      bw.write(sortedLibs.size() + " compound libraries");
//      bw.newLine();
//      while (!sortedLibs.isEmpty()) {
//        Library biggest = sortedLibs.pollLast();
//        JarSet mainSet = biggest.getJars();
//        bw.write("Listing FQNs for library found in " + mainSet.size() + " jars");
//        bw.newLine();
//        for (FqnFragment fqn : biggest.getFqns()) {
//          double percent = (double) fqn.getJars().getIntersectionSize(mainSet) / (double) fqn.getJars().size();
//          bw.write("  " + fqn.getFqn() + " " + fqn.getJars().size() + " " + format.format(percent));
//          bw.newLine();
//        }
//      }
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error printing statistics", e);
    }
    task.finish();
  }
}