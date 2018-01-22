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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.junit.Test;

public class TestJarFilter {

    @Test
    public void testJarFilter() throws URISyntaxException, IOException, OozieActionConfiguratorException {
        LinkedList<URI> listUris = new LinkedList<URI>();
        String sparkVersion = "2.1.0";
        String sparkYarnJar = "spark-yarn-" + sparkVersion + ".jar";
        String applicationJarName = "oozie-examples.jar";
        String renamedApplicationJar = "renamed-oozie-examples.jar";
        URI sparkYarnJarUri = new URI("hdfs://localhost:8020/user/sparkjars/" + sparkYarnJar);
        URI applicationJarUri = new URI("hdfs://localhost:8020/user/sparkdata/" + applicationJarName);
        createSparkYarnJar(sparkYarnJar, sparkVersion);
        populateUris(listUris, sparkYarnJarUri, applicationJarUri);

        // check application jar, spark yarn jar and spark version
        JarFilter jarFilter = new JarFilter(listUris, applicationJarUri.getPath());
        jarFilter.filter();
        assertEquals(applicationJarUri.toString(), jarFilter.getApplicationJar());
        assertEquals(sparkYarnJarUri.toString(), jarFilter.getSparkYarnJar());
        assertEquals(sparkVersion, jarFilter.getSparkVersion());
        checkFilteredUris(listUris, sparkYarnJarUri.toString(), applicationJarUri.toString());
        listUris.clear();

        // check application jar with fragmented URI
        applicationJarUri = new URI("hdfs", "localhost", "/user/sparkdata/" + applicationJarName,
                renamedApplicationJar);
        populateUris(listUris, sparkYarnJarUri, applicationJarUri);
        jarFilter = new JarFilter(listUris, applicationJarUri.getPath());
        jarFilter.filter();
        assertEquals(renamedApplicationJar, jarFilter.getApplicationJar());
        assertEquals(sparkYarnJarUri.toString(), jarFilter.getSparkYarnJar());
        assertEquals(sparkVersion, jarFilter.getSparkVersion());
        checkFilteredUris(listUris, sparkYarnJarUri.toString(), renamedApplicationJar);
        listUris.clear();

        // application jar is present in <file> with symlink
        // and user mentioned the symlink name in <jar>
        populateUris(listUris, sparkYarnJarUri, applicationJarUri);
        jarFilter = new JarFilter(listUris, renamedApplicationJar);
        jarFilter.filter();
        assertEquals(renamedApplicationJar, jarFilter.getApplicationJar());
        assertEquals(sparkYarnJarUri.toString(), jarFilter.getSparkYarnJar());
        assertEquals(sparkVersion, jarFilter.getSparkVersion());
        checkFilteredUris(listUris, sparkYarnJarUri.toString(), renamedApplicationJar);

        Files.deleteIfExists(new File(sparkYarnJar).toPath());
    }

    private void checkFilteredUris(LinkedList<URI> listUris, String sparkYarnJar, String applicationJar) {
        for(URI uri : listUris) {
            assertFalse(uri.toString().contains(sparkYarnJar));
            assertFalse(uri.toString().contains(applicationJar));
        }
    }

    private void createSparkYarnJar(String sparkYarnJar, String sparkVersion)
            throws FileNotFoundException, IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.SPECIFICATION_VERSION, sparkVersion);
        JarOutputStream target = new JarOutputStream(new FileOutputStream(sparkYarnJar), manifest);
        target.flush();
        target.close();
    }

    private void populateUris(LinkedList<URI> listUris, URI sparkYarnJarUri, URI applicationJarUri)
            throws URISyntaxException {
        String fileNamePattern = "hdfs://localhost:8020/user/sparkdata/%s%s%s";
        listUris.add(new URI(String.format(fileNamePattern, "_SUCCESS", "#", "ouputflag.txt")));
        listUris.add(new URI(String.format(fileNamePattern, "dependency.jar", "", "")));
        listUris.add(new URI(String.format(fileNamePattern, "helper.jar", "", "")));
        listUris.add(sparkYarnJarUri);
        listUris.add(applicationJarUri);
    }
}