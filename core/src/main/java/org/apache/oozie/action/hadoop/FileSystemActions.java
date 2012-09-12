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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Node;
/**
 * Class to perform file system operations specified in the prepare block of Workflow
 *
 */
public class FileSystemActions {
    private static Collection<String> supportedFileSystems;

    public FileSystemActions(Collection<String> fileSystems) {
        supportedFileSystems = fileSystems;
    }

    /**
     * Method to execute the prepare actions based on the command
     *
     * @param n Child node of the prepare XML
     * @throws LauncherException
     */
    public void execute(Node n) throws LauncherException {
        String command = n.getNodeName();
        if (command.equals("delete")) {
            delete(new Path(n.getAttributes().getNamedItem("path").getNodeValue().trim()));
        } else if (command.equals("mkdir")) {
            mkdir(new Path(n.getAttributes().getNamedItem("path").getNodeValue().trim()));
        }
    }

    // Method to delete the specified file based on the path
    private void delete(Path path) throws LauncherException {
        try {
            validatePath(path, true);
            FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
            if (fs.exists(path)) {
                if (!fs.delete(path, true)) {
                    String deleteFailed = "Deletion of path " + path.toString() + " failed.";
                    System.out.println(deleteFailed);
                    throw new LauncherException(deleteFailed);
                } else {
                    System.out.println("Deletion of path " + path.toString() + " was successful.");
                }
            }
        } catch (IOException ex) {
            throw new LauncherException(ex.getMessage(), ex);
        }

    }

    // Method to create a directory based on the path
    private void mkdir(Path path) throws LauncherException {
        try {
            validatePath(path, true);
            FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
            if (!fs.exists(path)) {
                if (!fs.mkdirs(path)) {
                    String mkdirFailed = "Creating directory at " + path + " failed.";
                    System.out.println(mkdirFailed);
                    throw new LauncherException(mkdirFailed);
                } else {
                    System.out.println("Creating directory at path " + path + " was successful.");
                }
            }
        } catch (IOException ex) {
            throw new LauncherException(ex.getMessage(), ex);
        }
    }

    // Method to validate the path provided for the prepare action
    private void validatePath(Path path, boolean withScheme) throws LauncherException {
        String scheme = path.toUri().getScheme();
        if (withScheme) {
            if (scheme == null) {
                String nullScheme = "Scheme of the path " + path + " is null";
                System.out.println(nullScheme);
                throw new LauncherException(nullScheme);
            } else if (supportedFileSystems.size() != 1 || !supportedFileSystems.iterator().next().equals("*")) {
                if (!supportedFileSystems.contains(scheme.toLowerCase())) {
                    String unsupportedScheme = "Scheme of '" + path + "' is not supported.";
                    System.out.println(unsupportedScheme);
                    throw new LauncherException(unsupportedScheme);
                }
            }
        } else if (scheme != null) {
            String notNullScheme = "Scheme of the path " + path + " is not null as specified.";
            System.out.println(notNullScheme);
            throw new LauncherException(notNullScheme);
        }
    }
}
