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
package org.apache.oozie.command.coord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

public class TestCoordSubmitXCommand extends XDataTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Basic test
     *
     * @throws Exception
     */
    public void testBasicSubmit() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        CoordinatorJobBean job = checkCoordJobs(jobId);
        if (job != null) {
            assertEquals(job.getTimeout(), Services.get().getConf().getInt(
                    "oozie.service.coord.normal.default.timeout", -2));
            assertEquals(job.getConcurrency(), Services.get().getConf().getInt(
                    "oozie.service.coord.default.concurrency", 1));
        }
    }

    /**
     * Testing for when user tries to submit a coordinator application having data-in events
     * that erroneously specify multiple input data instances inside a single <instance> tag.
     * Job gives submission error and indicates appropriate correction
     * Testing both negative(error) and well as positive(success) cases
     */
    public void testBasicSubmitWithMultipleInstancesInputEvent() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";

        // CASE 1: Failure case i.e. multiple data-in instances
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-input-instance1.xml", -1);
        Writer writer = new FileWriter(new URI(appPath).getPath());
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");

        try {
            sc.call();
            fail("Expected to catch errors due to incorrectly specified input data set instances");
        }
        catch (CommandException e) {
            assertEquals(sc.getJob().getStatus(), Job.Status.FAILED);
            assertEquals(e.getErrorCode(), ErrorCode.E1021);
            assertTrue(e.getMessage().contains(sc.COORD_INPUT_EVENTS) && e.getMessage().contains("per data-in instance"));
        }

        // CASE 2: Multiple data-in instances specified as separate <instance> tags, but one or more tags are empty. Check works for whitespace in the tags too
        reader = IOUtils.getResourceAsReader("coord-multiple-input-instance2.xml", -1);
        writer = new FileWriter(new URI(appPath).getPath());
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");

        try {
            sc.call();
            fail("Expected to catch errors due to incorrectly specified input data set instances");
        }
        catch (CommandException e) {
            assertEquals(sc.getJob().getStatus(), Job.Status.FAILED);
            assertEquals(e.getErrorCode(), ErrorCode.E1021);
            assertTrue(e.getMessage().contains(sc.COORD_INPUT_EVENTS) && e.getMessage().contains("is empty"));
        }

        // CASE 3: Success case i.e. Multiple data-in instances specified correctly as separate <instance> tags
        reader = IOUtils.getResourceAsReader("coord-multiple-input-instance3.xml", -1);
        writer = new FileWriter(new URI(appPath).getPath());
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");

        try {
            sc.call();
        }
        catch (CommandException e) {
            fail("Unexpected failure: " + e);
        }

        // CASE 4: Success case i.e. Single instances for input and single instance for output, but both with ","
        reader = IOUtils.getResourceAsReader("coord-multiple-input-instance4.xml", -1);
        writer = new FileWriter(new URI(appPath).getPath());
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");

        try {
            sc.call();
        }
        catch (CommandException e) {
            fail("Unexpected failure: " + e);
        }
    }

    /**
     * Testing for when user tries to submit a coordinator application having data-out events
     * that erroneously specify multiple output data instances inside a single <instance> tag.
     * Job gives submission error and indicates appropriate correction
     * Testing negative(error) cases as well as Positive(success) cases.
     */
    public void testBasicSubmitWithMultipleInstancesOutputEvent() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";

        // CASE 1: Failure case i.e. multiple data-out instances
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-output-instance1.xml", -1);
        Writer writer = new FileWriter(new URI(appPath).getPath());
        IOUtils.copyCharStream(reader, writer);

        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");

        try {
            sc.call();
            fail("Expected to catch errors due to incorrectly specified output data set instances");
        }
        catch (CommandException e) {
            assertEquals(sc.getJob().getStatus(), Job.Status.FAILED);
            assertEquals(e.getErrorCode(), ErrorCode.E1021);
            assertTrue(e.getMessage().contains(sc.COORD_OUTPUT_EVENTS) && e.getMessage().contains("per data-out instance"));
        }

        // CASE 2: Data-out instance tag is empty. Check works for whitespace in the tag too
        reader = IOUtils.getResourceAsReader("coord-multiple-output-instance2.xml", -1);
        writer = new FileWriter(new URI(appPath).getPath());
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");

        try {
            sc.call();
            fail("Expected to catch errors due to incorrectly specified output data set instances");
        }
        catch (CommandException e) {
            assertEquals(sc.getJob().getStatus(), Job.Status.FAILED);
            assertEquals(e.getErrorCode(), ErrorCode.E1021);
            assertTrue(e.getMessage().contains(sc.COORD_OUTPUT_EVENTS) && e.getMessage().contains("is empty"));
        }

        // CASE 3: Multiple <instance> tags within data-out should fail coordinator schema validation - different error than above is expected
        reader = IOUtils.getResourceAsReader("coord-multiple-output-instance3.xml", -1);
        writer = new FileWriter(new URI(appPath).getPath());
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");

        try {
            sc.call();
            fail("Expected to catch errors due to incorrectly specified output data set instances");
        }
        catch (CommandException e) {
            assertEquals(sc.getJob().getStatus(), Job.Status.FAILED);
            assertEquals(e.getErrorCode(), ErrorCode.E0701);
            assertTrue(e.getMessage().contains("No child element is expected at this point"));
        }

        // CASE 4: Success case, where only one instance is configured, but expression has a ","
        reader = IOUtils.getResourceAsReader("coord-multiple-output-instance4.xml", -1);
        writer = new FileWriter(new URI(appPath).getPath());
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");

        try {
            sc.call();
        }
        catch (CommandException e) {
            fail("Not expected to fail here");
        }
    }

    /**
     * Basic coordinator submit test with bundleId
     *
     * @throws Exception
     */
    public void testBasicSubmitWithBundleId() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());

        this.addRecordToBundleActionTable("OOZIE-B", "COORD-NAME", 0, Job.Status.PREP);

        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING", "OOZIE-B", "COORD-NAME");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        CoordinatorJobBean job = checkCoordJobs(jobId);
        if (job != null) {
            assertEquals("OOZIE-B", job.getBundleId());
            assertEquals("COORD-NAME", job.getAppName());
            assertEquals("uri:oozie:coordinator:0.2", job.getAppNamespace());
        } else {
            fail();
        }
    }

    /**
     * Basic coordinator submit test from bundle but with wrong namespace
     *
     * @throws Exception
     */
    public void testBasicSubmitWithWrongNamespace() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());

        this.addRecordToBundleActionTable("OOZIE-B", "COORD-NAME", 0, Job.Status.PREP);

        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING", "OOZIE-B", "COORD-NAME");
        try {
            sc.call();
            fail("Exception expected because namespace is too old when submit coordinator through bundle!");
        }
        catch (CommandException e) {
            // should come here for namespace errors
        }

    }

    /**
     * Basic test
     *
     * @throws Exception
     */
    public void testBasicSubmitWithSLA() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns='uri:oozie:coordinator:0.2' xmlns:sla='uri:oozie:sla:0.1'> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> "
                + " <sla:info>"
                + " <sla:app-name>test-app</sla:app-name>"
                + " <sla:nominal-time>${coord:nominalTime()}</sla:nominal-time>"
                + " <sla:should-start>${5 * MINUTES}</sla:should-start>"
                + " <sla:should-end>${2 * HOURS}</sla:should-end>"
                + " <sla:notification-msg>Notifying User for ${coord:nominalTime()} nominal time </sla:notification-msg>"
                + " <sla:alert-contact>abc@yahoo.com</sla:alert-contact>"
                + " <sla:dev-contact>abc@yahoo.com</sla:dev-contact>"
                + " <sla:qa-contact>abc@yahoo.com</sla:qa-contact>"
                + " <sla:se-contact>abc@yahoo.com</sla:se-contact>"
                + " <sla:alert-frequency>LAST_HOUR</sla:alert-frequency>"
                + " <sla:alert-percentage>10</sla:alert-percentage>" + "</sla:info>" + "</action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        checkCoordJobs(jobId);
    }

    /**
     * Use fixed values for frequency
     *
     * @throws Exception
     */
    public void testSubmitFixedValues() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"60\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"120\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        checkCoordJobs(jobId);
    }

    /**
     * test schema error. Negative test case.
     *
     * @throws Exception
     */
    public void testSchemaError() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequencyERROR=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"60\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"120\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");
        try {
            sc.call();
            fail("Exception expected if schema has errors!");
        }
        catch (CommandException e) {
            // should come here for schema errors
        }
    }

    /**
     * Don't include datasets, input-events, or output-events in XML.
     *
     * @throws Exception
     */
    public void testSubmitNoDatasets() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> "
                + "<controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        checkCoordJobs(jobId);
    }

    /**
     * Don't include username. Negative test case.
     *
     * @throws Exception
     */
    public void testSubmitNoUsername() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> "
                + "<controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        // conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");
        try {
            sc.call();
            fail("Exception expected if user.name is not set!");
        }
        catch (CommandException e) {
            // should come here
        }
    }

    /**
     * Don't include controls in XML.
     *
     * @throws Exception
     */
    public void testSubmitNoControls() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        checkCoordJobs(jobId);

    }

    /**
     * Test Done Flag in Schema
     *
     * @throws Exception
     */
    public void testSubmitWithDoneFlag() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> "
                + "<done-flag>consume_me</done-flag> </dataset>"
                + "<dataset name=\"local_b\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflowsb/${YEAR}/${DAY}</uri-template> "
                + "<done-flag>${MY_DONE_FLAG}</done-flag> </dataset>"
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "<data-in name=\"B\" dataset=\"local_b\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("MY_DONE_FLAG", "complete");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        checkCoordJobs(jobId);
    }

    /**
     * Test Done Flag in Schema
     *
     * @throws Exception
     */
    public void testSubmitWithVarAppName() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"${NAME}\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.3\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> "
                + "<done-flag>consume_me</done-flag> </dataset>"
                + "<dataset name=\"local_b\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflowsb/${YEAR}/${DAY}</uri-template> "
                + "<done-flag>${MY_DONE_FLAG}</done-flag> </dataset>"
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "<data-in name=\"B\" dataset=\"local_b\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("MY_DONE_FLAG", "complete");
        conf.set("NAME", "test_app_name");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        CoordinatorJobBean job = checkCoordJobs(jobId);
        if (job != null) {
            assertEquals(job.getAppName(), "test_app_name");
        }
    }

    /**
     * Don't include controls in XML.
     *
     * @throws Exception
     */
    public void testSubmitReservedVars() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("MINUTES", "1");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");

        try {
            sc.call();
            fail("Coord job submission should fail with reserved variable definitions.");
        }
        catch (CommandException ce) {

        }
    }

    /**
     * Checking that any dataset initial-instance is not set to a date earlier than the server default Jan 01, 1970 00:00Z UTC
     * @throws Exception
     */
    public void testSubmitDatasetInitialInstance() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        Reader reader = IOUtils.getResourceAsReader("coord-dataset-initial-instance.xml", -1);
        Writer writer = new FileWriter(new URI(appPath).getPath());
        IOUtils.copyCharStream(reader, writer);

        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");
        try {
            sc.call();
            fail("Expected to catch errors due to invalid dataset initial instance");
        }
        catch(CommandException cx) {
            assertEquals(sc.getJob().getStatus(), Job.Status.FAILED);
            assertEquals(cx.getErrorCode(), ErrorCode.E1021);
            if(!(cx.getMessage().contains("earlier than the default initial instance"))) {
                fail("Unexpected failure - " + cx.getMessage());
            }
        }
    }

    private void _testConfigDefaults(boolean withDefaults) throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"${startTime}\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());

        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "UNIT_TESTING");

        if (withDefaults) {
            String defaults = "<configuration><property><name>startTime</name>" +
                              "<value>2009-02-01T01:00Z</value></property></configuration>";
            writeToFile(defaults, getTestCaseDir() + File.separator + CoordSubmitXCommand.CONFIG_DEFAULT);
            String jobId = sc.call();
            assertEquals(jobId.substring(jobId.length() - 2), "-C");
        }
        else {
            try {
                sc.call();
                fail();
            }
            catch (CommandException ex) {
                assertEquals(ErrorCode.E1004, ex.getErrorCode());
            }
            catch (Exception ex) {
                fail();
            }
        }
    }

    public void testMissingConfigDefaults() throws Exception {
        _testConfigDefaults(false);
    }

    public void testAvailConfigDefaults() throws Exception {
        _testConfigDefaults(true);
    }

    /**
     * Helper methods
     *
     * @param jobId
     */
    private CoordinatorJobBean checkCoordJobs(String jobId) {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorJobBean job = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
            return job;
        }
        catch (JPAExecutorException e) {
            fail("Job ID " + jobId + " was not stored properly in db");
        }
        return null;
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
}
