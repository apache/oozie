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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.junit.rules.TemporaryFolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestLauncherMain {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private PrintStream originalStream;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();
    @Before
    public void setUpStreams() {
        originalStream = System.out;
        System.setOut(new PrintStream(outContent));
    }

    @After
    public void cleanUpStreams() {
        System.setOut(originalStream);
    }

    @Test
    public void testLog4jPropertiesPresentAndReadable() {
        final LauncherMain noop = new NoopLauncherMain();
        noop.setupLog4jProperties();

        assertTrue(outContent.toString().contains("INFO: log4j config file log4j.properties loaded successfully."));
        assertEquals(noop.log4jProperties.size(), 5);
    }

    private static class NoopLauncherMain extends LauncherMain {
        @Override
        protected void run(String[] args) throws Exception {}
    }

    @Test
    public void testDontCreateStreamIfFileExists() throws IOException {
        File f = tmp.newFile();
        try (FileOutputStream fos = new FileOutputStream(f)) {
            fos.write("foo".getBytes());
        }

        try (FileOutputStream fos = LauncherMain.createStreamIfFileNotExists(f)) {
            assertNull(fos);
        }
    }

    @Test
    public void testConfigWrite() throws IOException {
        File f = new File(tmp.newFolder(), "nonExistentFile");
        assertFalse(f.exists());
        try (FileOutputStream fos = LauncherMain.createStreamIfFileNotExists(f)) {
            Configuration c = new Configuration(false);
            c.set("foo", "bar");
            c.writeXml(fos);
        }
        String contents = new String(Files.readAllBytes(f.toPath()));
        assertTrue(contents.contains("foo"));
        assertTrue(contents.contains("bar"));
        assertTrue(contents.contains("<configuration>"));
        assertTrue(contents.contains("<property"));
    }

    @Test
    public void testPropertiesWrite() throws IOException {
        File f = new File(tmp.newFolder(), "nonExistentFile");
        assertFalse(f.exists());
        try (FileOutputStream fos = LauncherMain.createStreamIfFileNotExists(f)) {
            Properties p = new Properties();
            p.setProperty("foo", "bar");
            p.store(fos, "");
        }
        String contents = new String(Files.readAllBytes(f.toPath()));
        assertTrue(contents.contains("foo=bar"));
    }

    @Test
    public void testKillChildYarnJobs() throws Exception {
        YarnClient yc = Mockito.mock(YarnClient.class);
        ApplicationReport ar = Mockito.mock(ApplicationReport.class);
        Mockito.when(yc.getApplicationReport(Mockito.any(ApplicationId.class))).thenReturn(ar);

        Mockito.when(ar.getFinalApplicationStatus())
                .thenReturn(FinalApplicationStatus.UNDEFINED)
                .thenReturn(FinalApplicationStatus.FAILED)
                .thenReturn(FinalApplicationStatus.KILLED);

        ApplicationId appz[] = {
                ApplicationId.newInstance(System.currentTimeMillis(), 1),
                ApplicationId.newInstance(System.currentTimeMillis(), 2),
                ApplicationId.newInstance(System.currentTimeMillis(), 3)
        };

        Collection<ApplicationId> result = LauncherMain.checkAndKillChildYarnJobs(yc, null, Arrays.asList(appz));

        assertEquals(1, result.size());
        assertEquals(appz[0].getId(), result.iterator().next().getId());
    }
}
