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

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;

/**
 * This class is used for filtering out unwanted jars.
 */
class JarFilter {
    private String sparkVersion = "1.X.X";
    private String sparkYarnJar;
    private String applicationJar;
    private Collection<URI> listUris = null;

    /**
     * @param listUris List of URIs to be filtered
     * @param jarPath Application jar
     * @throws IOException
     * @throws URISyntaxException
     */
    JarFilter(final Collection<URI> listUris, final String jarPath) throws URISyntaxException, IOException {
        this.listUris = listUris;
        applicationJar = jarPath;
        final Path p = new Path(jarPath);
        if (p.isAbsolute()) {
            applicationJar = HadoopUriFinder.getFixedUri(p.toUri()).toString();
        }
    }

    /**
     * Filters out the Spark yarn jar and application jar. Also records
     * spark yarn jar's version.
     *
     * @throws OozieActionConfiguratorException
     */
    void filter() throws OozieActionConfiguratorException {
        final Iterator<URI> iterator = listUris.iterator();
        File matchedFile = null;
        final Path applJarPath = new Path(applicationJar);
        while (iterator.hasNext()) {
            final URI uri = iterator.next();
            final Path p = new Path(uri);
            if (SparkMain.SPARK_YARN_JAR_PATTERN.matcher(p.getName()).find()) {
                matchedFile = SparkMain.getMatchingFile(SparkMain.SPARK_YARN_JAR_PATTERN);
            }
            else if (SparkMain.SPARK_ASSEMBLY_JAR_PATTERN.matcher(p.getName()).find()) {
                matchedFile = SparkMain.getMatchingFile(SparkMain.SPARK_ASSEMBLY_JAR_PATTERN);
            }
            if (matchedFile != null) {
                sparkYarnJar = uri.toString();
                try {
                    sparkVersion = HadoopUriFinder.getJarVersion(matchedFile);
                    System.out.println("Spark Version " + sparkVersion);
                }
                catch (final IOException io) {
                    System.out.println(
                            "Unable to open " + matchedFile.getPath() + ". Default Spark Version " + sparkVersion);
                }
                iterator.remove();
                matchedFile = null;
            }
            // Here we skip the application jar, because
            // (if uris are same,) it will get distributed multiple times
            // - one time with --files and another time as application jar.
            if (isApplicationJar(p.getName(), uri, applJarPath)) {
                final String fragment = uri.getFragment();
                applicationJar = fragment != null && fragment.length() > 0 ? fragment : uri.toString();
                iterator.remove();
            }
        }
    }

    /**
     * Checks if a file is application jar
     *
     * @param fileName fileName name of the file
     * @param fileUri fileUri URI of the file
     * @param applJarPath Path of application jar
     * @return true if fileName or fileUri is the application jar
     */
    private boolean isApplicationJar(final String fileName, final URI fileUri, final Path applJarPath) {
        return (fileName.equals(applicationJar) || fileUri.toString().equals(applicationJar)
                || applJarPath.getName().equals(fileName)
                || applicationJar.equals(fileUri.getFragment()));
    }

    String getApplicationJar() {
        return applicationJar;
    }

    String getSparkYarnJar() {
        return sparkYarnJar;
    }

    String getSparkVersion() {
        return sparkVersion;
    }
}
