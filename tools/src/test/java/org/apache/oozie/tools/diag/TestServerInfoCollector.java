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

package org.apache.oozie.tools.diag;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class TestServerInfoCollector {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Mock
    private DiagOozieClient mockDiagClient;
    private File testTempFolder;
    private ServerInfoCollector serverInfoCollector;

    @Before
    public void setup() throws IOException {
        testTempFolder = folder.newFolder();
        serverInfoCollector = new ServerInfoCollector(mockDiagClient);
    }

    @Test
    public void testGetShareLibInfo() throws Exception {
        doReturn("share1\nshare2").when(mockDiagClient).listShareLib(anyString());

        serverInfoCollector.storeShareLibInfo(testTempFolder);

        final File shareLibOut = new File(testTempFolder, "sharelib.txt");
        assertTrue(shareLibOut.exists());
        assertFileContains(shareLibOut, "share1");
    }

    @Test
    public void testGetJavaSystemProperties() throws Exception {
        Map<String, String> testSysProps = new HashMap<>();
        testSysProps.put("javax.xml.parsers.DocumentBuilderFactory", "org.apache.xerces.jaxp.DocumentBuilderFactoryImpl");
        doReturn(testSysProps).when(mockDiagClient).getJavaSystemProperties();

        serverInfoCollector.storeJavaSystemProperties(testTempFolder);

        final File shareLibOut = new File(testTempFolder, "java-sys-props.txt");
        assertTrue(shareLibOut.exists());
        assertFileContains(shareLibOut, "DocumentBuilderFactory");
    }

    @Test
    public void testGetOsEnv() throws Exception {
        Map<String, String> testSysProps = new HashMap<>();
        testSysProps.put("JETTY_OUT", "jetty.out");
        doReturn(testSysProps).when(mockDiagClient).getOSEnv();

        serverInfoCollector.storeOsEnv(testTempFolder);

        final File shareLibOut = new File(testTempFolder, "os-env-vars.txt");
        assertTrue(shareLibOut.exists());
        assertFileContains(shareLibOut, "JETTY_OUT");
    }

    @Test
    public void testGetServerConfiguration() throws Exception {
        final Map<String, String> testSysProps = new HashMap<>();
        testSysProps.put("oozie.services", "org.apache.oozie.service.SchedulerService");
        doReturn(testSysProps).when(mockDiagClient).getServerConfiguration();

        serverInfoCollector.storeServerConfiguration(testTempFolder);

        final File shareLibOut = new File(testTempFolder, "effective-oozie-site.xml");
        assertTrue(shareLibOut.exists());
        assertFileContains(shareLibOut, "SchedulerService");
    }

    @Test
    public void testGetCallableQueueDump() throws Exception {
        final List<String> testDump = Arrays.asList("(coord_action_start,1)","(coord_action_start,1)");
        doReturn(testDump).when(mockDiagClient).getQueueDump();

        serverInfoCollector.storeCallableQueueDump(testTempFolder);

        final File shareLibOut = new File(testTempFolder, "queue-dump.txt");
        assertTrue(shareLibOut.exists());
        assertFileContains(shareLibOut, "coord_action_start");
    }

    static void assertFileContains(final File testFile, final String testData) throws IOException {
        final String str = new String(Files.readAllBytes(testFile.toPath()), StandardCharsets.UTF_8.toString());
        assertTrue(str.contains(testData));
    }
}