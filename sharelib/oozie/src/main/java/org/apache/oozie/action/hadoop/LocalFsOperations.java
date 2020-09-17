/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.action.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;

public class LocalFsOperations {
    private static final int WALK_DEPTH = 2;

    /**
     * Reads the launcher configuration "launcher.xml"
     * @return Configuration object
     */
    public Configuration readLauncherConf() {
        File confFile = new File(LauncherAM.LAUNCHER_JOB_CONF_XML);
        Configuration conf = new Configuration(false);
        conf.addResource(new org.apache.hadoop.fs.Path(confFile.getAbsolutePath()));
        return conf;
    }

    /**
     * Print files and directories in current directory. Will list files in the sub-directory (only 2 level deep)
     * @param folder the target folder
     * @throws IOException if the contents could not be listed
     */
    public void printContentsOfDir(File folder) throws IOException {
        System.out.println();
        System.out.println("Files in current dir:" + folder.getAbsolutePath());
        System.out.println("======================");

        final Path root = folder.toPath();
        Files.walkFileTree(root, EnumSet.of(FileVisitOption.FOLLOW_LINKS), WALK_DEPTH, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (attrs.isRegularFile()) {
                    System.out.println("  File: " + root.relativize(file));
                } else if (attrs.isDirectory()) {
                    System.out.println("  Dir: " +  root.relativize(file) + "/");
                }

                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Returns the contents of a file as string.
     *
     * @param file the File object which represents the file to be read
     * @param type Type of the file
     * @param maxLen Maximum allowed length
     * @return The file contents as string
     * @throws IOException if the file is bigger than maxLen or there is any I/O error
     * @throws FileNotFoundException if the file does not exist
     */
    public String getLocalFileContentAsString(File file, String type, int maxLen) throws IOException {
        if (file.exists()) {
            if (maxLen > -1 && file.length() > maxLen) {
                throw new IOException(type + " data exceeds its limit [" + maxLen + "]");
            }

            return com.google.common.io.Files.toString(file, StandardCharsets.UTF_8);
        } else {
            throw new FileNotFoundException("File not found: " + file.toPath().toAbsolutePath());
        }
    }

    /**
     * Checks if a given File exists or not. This method helps writing unit tests.
     * @param file to check whether it exists or not
     * @return true if file exists
     */
    public boolean fileExists(File file) {
        return file.exists();
    }
}