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
package edu.uci.ics.sourcerer.extractor.io.file;

import static edu.uci.ics.sourcerer.util.io.Logging.logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;

import edu.uci.ics.sourcerer.extractor.io.IExtractorWriter;
import edu.uci.ics.sourcerer.repo.base.Repository;
import edu.uci.ics.sourcerer.util.io.Property;
import edu.uci.ics.sourcerer.util.io.PropertyManager;

/**
 * @author Joel Ossher (jossher@uci.edu)
 */
public abstract class ExtractorWriter implements IExtractorWriter {
  private BufferedWriter writer;
  private Repository input;
  
  protected ExtractorWriter(Repository input, Property property) {
    this.input = input;
    PropertyManager properties = PropertyManager.getProperties();
    try {
      writer = new BufferedWriter(new FileWriter(new File(properties.getValueAsFile(Property.OUTPUT), properties.getValue(property))));
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error opening file", e);
    }
  }
  
  protected String convertToRelativePath(String path) {
    return input.convertToRelativePath(path);
  }
  
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error closing writer", e);
    }
  }
  
  protected void write(String line) {
    try {
      writer.write(line);
      writer.write("\n");
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Error writing line to file", e);
    }
  }
}