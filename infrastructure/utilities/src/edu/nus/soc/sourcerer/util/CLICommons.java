package edu.nus.soc.sourcerer.util;

import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class CLICommons {
  
  public static final String EXEC = "EXECUTABLE";

  /**
   * Parse the command line parameters.
   *
   * @param args The parameters to parse.
   * @param options a set of predefined options.
   * @return The parsed command line.
   * @throws org.apache.commons.cli.ParseException When the parsing of the parameters fails.
   */
  public static CommandLine parseArgs(String[] args, Options options)
      throws ParseException {
    Option o = new Option("h", "help", false,
        "print usage help");
    o.setRequired(false);
    options.addOption(o);
    
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      
      if (cmd.hasOption("h")) {
        printHelp(options, null);
        System.exit(0);
      }
    } catch (ParseException e) {
      printHelp(options, e);
      System.exit(-1);
    }

    return cmd;
  }
  
  /**
   * Prints usage help. Pass null for the second parameter for no error message
   * or pass an exception to print its message and switch to standard error.
   * 
   * @param options
   * @param e
   */
  public static void printHelp(Options options, Exception e) {
    String label = "", msg = "";
    PrintStream std = System.out;
    
    if (e != null) {
      label = "ERROR: ";
      msg = e.getMessage();
      std = System.err;
    }
    
    std.println(label + msg + "\n");
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(EXEC, options, true);
  }
}
