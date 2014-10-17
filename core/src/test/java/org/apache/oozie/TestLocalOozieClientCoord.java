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

package org.apache.oozie;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;

public class TestLocalOozieClientCoord extends XDataTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        /*
         * DB cleanup below is needed to clean up the job records possibly left by previously executed tests.
         * For example, such records are left by test org.apache.oozie.executor.jpa.TestCoordActionUpdateJPAExecutor.
         * Note that by default Oozie tests are executed in "filesystem" (in fact, arbitrary) order. This way problems caused
         * by left records can be flaky (not reproduced constantly).
         * Services re-init is needed for the DB cleanup.
         */
        services = new Services();
        services.init();

        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    public void testGetOozieUrl() {
        OozieClient client = LocalOozie.getCoordClient();
        assertEquals("localoozie", client.getOozieUrl());
    }

    public void testGetProtocolUrl() throws IOException, OozieClientException {
        OozieClient client = LocalOozie.getCoordClient();
        assertEquals("localoozie", client.getProtocolUrl());
    }

    public void testValidateWSVersion() throws IOException, OozieClientException {
        OozieClient client = LocalOozie.getCoordClient();
        client.validateWSVersion();
    }

    public void testHeaderMethods() {
        OozieClient client = LocalOozie.getCoordClient();
        client.setHeader("h", "v");
        assertNull(client.getHeader("h"));
        Iterator<String> hit = client.getHeaderNames();
        assertFalse(hit.hasNext());
        try {
            hit.next();
            fail("NoSuchElementException expected.");
        }
        catch (NoSuchElementException nsee) {
            // expected
        }
        client.removeHeader("h");
        assertNull(client.getHeader("h"));
    }

    public void testGetJobsInfo() {
        OozieClient client = LocalOozie.getCoordClient();
        try {
            client.getJobsInfo("foo");
            fail("OozieClientException expected.");
        }
        catch (OozieClientException oce) {
            assertEquals(ErrorCode.E0301.toString(), oce.getErrorCode());
        }
        try {
            client.getJobsInfo("foo", 0, 5);
            fail("OozieClientException expected.");
        }
        catch (OozieClientException oce) {
            assertEquals(ErrorCode.E0301.toString(), oce.getErrorCode());
        }
        try {
            client.getJobInfo("foo-id");
            fail("OozieClientException expected.");
        }
        catch (OozieClientException oce) {
            assertEquals(ErrorCode.E0301.toString(), oce.getErrorCode());
        }
    }

    public void testReRun2() {
        OozieClient client = LocalOozie.getCoordClient();
        try {
            client.reRun("foo-id", client.createConfiguration());
            fail("OozieClientException expected.");
        }
        catch (OozieClientException oce) {
            assertEquals(ErrorCode.E0301.toString(), oce.getErrorCode());
        }
    }

    private void writeToFile(String appXml, String appPath) throws Exception {
        File wf = new File(new URI(appPath).getPath());
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(appXml);
        }
        catch (IOException iex) {
            throw iex;
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }

    public void testJobMethods() throws Exception {
        final OozieClient client = LocalOozie.getCoordClient();

        // Just in case, check that there are no Coord job records left by previous tests:
        List<CoordinatorJob> list0 = client.getCoordJobsInfo("", 1, 100);
        assertEquals(0, list0.size());

        Properties conf = client.createConfiguration();

        String appPath = getTestCaseFileUri("coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:minutes(20)}\" "
                + "start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>1</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:minutes(20)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:minutes(20)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template> </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  " + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> "
                + "<app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);

        conf.setProperty(OozieClient.COORDINATOR_APP_PATH, appPath);
        String jobId0 = client.submit(conf);
        client.kill(jobId0);

        String jobId = client.run(conf);
        client.suspend(jobId);
        client.resume(jobId);
        client.kill(jobId);

        CoordinatorJob job = client.getCoordJobInfo(jobId);
        String appName = job.getAppName();
        assertEquals("NAME", appName);

        List<CoordinatorJob> list = client.getCoordJobsInfo("", 1, 5);
        assertEquals(2, list.size());
    }
}
