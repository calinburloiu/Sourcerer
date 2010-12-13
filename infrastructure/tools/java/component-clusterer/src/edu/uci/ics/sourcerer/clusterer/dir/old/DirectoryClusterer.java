///* 
// * Sourcerer: an infrastructure for large-scale source code analysis.
// * Copyright (C) by contributors. See CONTRIBUTORS.txt for full list.
// *
// * This program is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
//
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU General Public License for more details.
//
// * You should have received a copy of the GNU General Public License
// * along with this program. If not, see <http://www.gnu.org/licenses/>.
// */
//package edu.uci.ics.sourcerer.clusterer.dir.old;
//
//import static edu.uci.ics.sourcerer.repo.general.AbstractRepository.INPUT_REPO;
//import static edu.uci.ics.sourcerer.util.io.Logging.logger;
//import static edu.uci.ics.sourcerer.util.io.Properties.INPUT;
//import static edu.uci.ics.sourcerer.util.io.Properties.OUTPUT;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Comparator;
//import java.util.Deque;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.NoSuchElementException;
//import java.util.Set;
//import java.util.TreeSet;
//import java.util.logging.Level;
//
//import edu.uci.ics.sourcerer.clusterer.stats.Matching;
//import edu.uci.ics.sourcerer.repo.base.IDirectory;
//import edu.uci.ics.sourcerer.repo.base.IJavaFile;
//import edu.uci.ics.sourcerer.repo.base.RepoProject;
//import edu.uci.ics.sourcerer.repo.base.Repository;
//import edu.uci.ics.sourcerer.util.Counter;
//import edu.uci.ics.sourcerer.util.Helper;
//import edu.uci.ics.sourcerer.util.Triplet;
//import edu.uci.ics.sourcerer.util.io.FileUtils;
//import edu.uci.ics.sourcerer.util.io.Property;
//import edu.uci.ics.sourcerer.util.io.TablePrettyPrinter;
//import edu.uci.ics.sourcerer.util.io.properties.IntegerProperty;
//import edu.uci.ics.sourcerer.util.io.properties.StringProperty;
//
///**
// * @author Joel Ossher (jossher@uci.edu)
// */
//public class DirectoryClusterer {
//  
//  
//  
//  
//  public static final Property<String> RANKED_DIRECTORIES = new StringProperty("ranked-directories", "ranked-dirs.txt", "The matched directories, ranked by popularity.");
//
//  private static Map<String, Directory> loadDirectoryMap() {
//    Map<String, Directory> dirs = Helper.newHashMap();
//    BufferedReader br = null;
//    try {
//      br = new BufferedReader(new FileReader(new File(INPUT.getValue(), DIRECTORY_LISTING.getValue())));
//      for (String line = br.readLine(); line != null; line = br.readLine()) {
//        String[] parts = line.split(" ");
//        Arrays.sort(parts, 2, parts.length);
//        DirectoryMatchedFile[] files = new DirectoryMatchedFile[parts.length - 2];
//        for (int i = 2; i < parts.length; i++) {
//          files[i - 2] = new DirectoryMatchedFile(parts[i]);
//        }
//        dirs.put(parts[1], new Directory(parts[0], parts[1], files));
//      }
//    } catch (IOException e) {
//      logger.log(Level.SEVERE, "Error loading directory listing", e);
//    } finally {
//      FileUtils.close(br);
//    }
//    return dirs;
//  }
//  
//  
//  
//  public static void generateComparisonFiles() {
//    logger.info("Loading directory listing...");
//    ArrayList<Directory> dirs = loadDirectoryListing();
//
//    TablePrettyPrinter printer = TablePrettyPrinter.getTablePrettyPrinter(POPULAR_NAMES);
//    logger.info("Calculating name popularity");
//    Collection<Counter<String>> names = computeNamePopularity(dirs);
//    printer.addHeader("Filenames sorted by popularity");
//    printer.beginTable(2);
//    printer.addDividerRow();
//    printer.addRow("Filename", "Count");
//    printer.addDividerRow();
//    for (Counter<String> name : names) {
//      printer.beginRow();
//      printer.addCell(name.getObject());
//      printer.addCell(name.getCount());
//    }
//    printer.addDividerRow();
//    printer.endTable();
//    printer.close();
//    
//    // Build the set of filenames to ignore
//    Set<String> ignore = Helper.newHashSet();
//    for (Counter<String> name : names) {
//      if (name.getCount() >= POPULAR_DISCARD.getValue()) {
//        ignore.add(name.getObject());
//      } else {
//        break;
//      }
//    }
//    names.clear();
//    
//    BufferedWriter matchedDirs = null;
//    BufferedWriter matchedFiles = null;
//    try {
//      matchedDirs = new BufferedWriter(new FileWriter(new File(OUTPUT.getValue(), MATCHED_DIRECTORIES.getValue())));
//      matchedFiles = new BufferedWriter(new FileWriter(new File(OUTPUT.getValue(), MATCHED_FILES.getValue())));
//      logger.info("Performing pairwise comparison...");
//      for (int i = 0; i < dirs.size(); i++) {
//        Directory dir = dirs.get(i);
//        logger.info("  Comparing dir from project " + dir + " (" + (i + 1) + " of " + dirs.size() + ")");
//        for (int j = i + 1; j < dirs.size(); j++) {
//          dir.compare(dirs.get(j), ignore);
//        }
//        // write out the dir info
//        matchedDirs.write(dir.toMatchedDirLine());
//        matchedDirs.write("\n");
//        // write out the file
//        for (DirectoryMatchedFile file : dir.getFiles()) {
//          matchedFiles.write(dir.getProject() + " " + dir.getPath() + " " + file.toCopiedFileLine() + "\n");
//        }
//      }
//      logger.info("Done!");
//    } catch (IOException e) {
//      logger.log(Level.SEVERE, "Exception!", e);
//    } finally {
//      FileUtils.close(matchedDirs);
//      FileUtils.close(matchedFiles);
//    }
//  }
//  
//  public static Triplet<Matching, Matching, Matching> getMatchings() {
//    return null;
//  }
//  
//  public static void compileStatistics() {
//    // Find the proportion of directories that are copied
//    {
//      int dirCount = 0;
//      
//      Set<Directory> rankedBy30 = Helper.newTreeSet(new Comparator<Directory>() {
//        @Override
//        public int compare(Directory o1, Directory o2) {
//          if (o2.get30() == o1.get30()) {
//            return o1.getPath().compareTo(o2.getPath());
//          } else {
//            return o2.get30() - o1.get30();
//          }
//        }});
//      Set<Directory> rankedBy50 = Helper.newTreeSet(new Comparator<Directory>() {
//        @Override
//        public int compare(Directory o1, Directory o2) {
//          if (o2.get50() == o1.get50()) {
//            return o1.getPath().compareTo(o2.getPath());
//          } else {
//            return o2.get50() - o1.get50();
//          }
//        }});
//      Set<Directory> rankedBy80 = Helper.newTreeSet(new Comparator<Directory>() {
//        @Override
//        public int compare(Directory o1, Directory o2) {
//          if (o2.get80() == o1.get80()) {
//            return o1.getPath().compareTo(o2.getPath());
//          } else {
//            return o2.get80() - o1.get80();
//          }
//        }});
//      
//      for (Directory dir : Directory.loadMatchedDirectories(new File(INPUT.getValue(), MATCHED_DIRECTORIES.getValue()))) {
//        dirCount++;
//        if (dir.matched30()) {
//          rankedBy30.add(dir);
//        }
//        if (dir.matched50()) {
//          rankedBy50.add(dir);
//        }
//        if (dir.matched80()) {
//          rankedBy80.add(dir);
//        }
//      }
//      logger.info(rankedBy30.size() + " of " + dirCount + " directories matched at 30%");
//      logger.info(rankedBy50.size() + " of " + dirCount + " directories matched at 50%");
//      logger.info(rankedBy80.size() + " of " + dirCount + " directories matched at 80%");
//
//      TablePrettyPrinter printer = TablePrettyPrinter.getTablePrettyPrinter(RANKED_DIRECTORIES);
//      printer.beginTable(3);
//      printer.addHeader("Directories matched at 30%");
//      printer.addDividerRow();
//      printer.addRow("Project", "Path", "Count");
//      printer.addDividerRow();
//      for (Directory dir : rankedBy30) {
//        printer.beginRow();
//        printer.addCell(dir.getProject());
//        printer.addCell(dir.getPath());
//        printer.addCell(dir.get30());
//      }
//      printer.addDividerRow();
//      printer.endTable();
//      
//      printer.beginTable(3);
//      printer.addHeader("Directories matched at 50%");
//      printer.addDividerRow();
//      printer.addRow("Project", "Path", "Count");
//      printer.addDividerRow();
//      for (Directory dir : rankedBy50) {
//        printer.beginRow();
//        printer.addCell(dir.getProject());
//        printer.addCell(dir.getPath());
//        printer.addCell(dir.get50());
//      }
//      printer.addDividerRow();
//      printer.endTable();
//      
//      printer.beginTable(3);
//      printer.addHeader("Directories matched at 80%");
//      printer.addDividerRow();
//      printer.addRow("Project", "Path", "Count");
//      printer.addDividerRow();
//      for (Directory dir : rankedBy80) {
//        printer.beginRow();
//        printer.addCell(dir.getProject());
//        printer.addCell(dir.getPath());
//        printer.addCell(dir.get80());
//      }
//      printer.addDividerRow();
//      printer.endTable();
//      
//      printer.close();
//    }
//    
//    // Find the proportion of files that are copied
//    {
//      int fileCount = 0;
//      int matched30 = 0;
//      int matched50 = 0;
//      int matched80 = 0;
//      for (DirectoryMatchedFile file : DirectoryMatchedFile.loadMatchedFiles(new File(INPUT.getValue(), MATCHED_FILES.getValue()))) {
//        fileCount++;
//        if (file.matched30()) {
//          matched30++;
//        }
//        if (file.matched50()) {
//          matched50++;
//        }
//        if (file.matched80()) {
//          matched80++;
//        }
//      }
//      logger.info(matched30 + " of " + fileCount + " files matched at 30%");
//      logger.info(matched50 + " of " + fileCount + " files matched at 50%");
//      logger.info(matched80 + " of " + fileCount + " files matched at 80%");
//    }
//  }
//  
//  public static void interactiveResultsViewer() {
//    System.out.println("Loading directory map...");
//    Map<String, Directory> dirs = loadDirectoryMap();
//    System.out.println("Calculating name popularity...");
//    Collection<Counter<String>> names = computeNamePopularity(dirs.values());
//    // Build the set of filenames to ignore
//    Set<String> ignore = Helper.newHashSet();
//    for (Counter<String> name : names) {
//      if (name.getCount() >= POPULAR_DISCARD.getValue()) {
//        ignore.add(name.getObject());
//      } else {
//        break;
//      }
//    }
//    names.clear();
//    
//    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//    try {
//      while (true) {
//        System.out.print("Please enter the directory path: ");
//        String path = br.readLine();
//        Directory dir = dirs.get(path);
//        if (dir == null) {
//          System.out.println("Unable to find: " + path + "\n");
//        } else {
//          System.out.println("What threshold would you like to view?");
//          double threshold = 0;
//          while (true) {
//            try {
//              threshold = Double.parseDouble(br.readLine());
//              if (threshold >= 0 && threshold <= 1) {
//                break;
//              } else {
//                System.out.println("Pick something between 0 and 1");  
//              }
//            } catch (NumberFormatException e) {
//              System.out.println("Invalid value, try again!");
//            }
//          }
//          System.out.println("Calculating matches...");
//          for (Directory other : dirs.values()) {
//            Collection<String> matches = dir.matches(other, ignore, threshold);
//            if (!matches.isEmpty()) {
//              System.out.println("  " + other.getPath());
//              for (String match : matches) {
//                System.out.println("    " + match);
//              }
//            }
//          }
//        }
//      }
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
//}