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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.util.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class SharelibUtils {

    private static String findDir(String name) throws Exception {
        return findDir(new File("foo").getAbsoluteFile().getParent(), name);
    }

    private static String findDir(String baseDir, String name) throws Exception {
        File dir = new File(baseDir, name).getAbsoluteFile();
        if (!dir.exists()) {
            File parent = dir.getParentFile().getParentFile();
            if (parent != null) {
                return findDir(parent.getAbsolutePath(), name);
            }
            else {
                throw new RuntimeException("Sharelib dir not found: " + name);
            }
        }
        return dir.getAbsolutePath();
    }

    private static String findClasspathFile(String sharelib) throws Exception {
        String classpathFile = null;
        String sharelibDir = findDir("sharelib");
        if (sharelibDir != null) {
            File file = new File(new File(new File(sharelibDir, sharelib), "target"), "classpath");
            if (file.exists()) {
                classpathFile = file.getAbsolutePath();
            }
            else {
                throw new RuntimeException("Sharelib classpath file for '" + sharelib +
                "' not found, Run 'mvn generate-test-resources' from Oozie source root");
            }
        }
        return classpathFile;
    }

    private static String[] getSharelibJars(String sharelib) throws Exception {
        String classpathFile = findClasspathFile(sharelib);
        BufferedReader br = new BufferedReader(new FileReader(classpathFile));
        String line = br.readLine();
        br.close();
        return line.split(System.getProperty("path.separator"));
    }

    private static Path[] copySharelibJarsToFileSytem(String sharelib, FileSystem fs, Path targetDir) throws Exception {
        String[] jars = getSharelibJars(sharelib);
        List<Path> paths = new ArrayList<Path>();
        for (String jar : jars) {
            if (jar.endsWith(".jar")) {
                Path targetPath = new Path(targetDir, new File(jar).getName());
                InputStream is = new FileInputStream(jar);
                OutputStream os = fs.create(targetPath);
                IOUtils.copyStream(is, os);
                paths.add(targetPath);
            }
        }
        return paths.toArray(new Path[paths.size()]);
    }

    public static void addToDistributedCache(String sharelib, FileSystem fs, Path targetDir,
                                             Configuration conf) throws Exception {
        Path[] paths = copySharelibJarsToFileSytem(sharelib, fs, targetDir);
        for (Path path : paths) {
            DistributedCache.addFileToClassPath(path, conf, fs);
        }
    }

}
