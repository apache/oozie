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
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLACalcStatus;
import org.apache.oozie.sla.SLACalculator;
import org.apache.oozie.sla.SLAOperations;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;

public class TestCoordSubmitXCommand extends XDataTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.getConf().set(Services.CONF_SERVICE_EXT_CLASSES,
                EventHandlerService.class.getName() + "," + SLAService.class.getName());
        services.init();
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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"${appName}-foo\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("appName", "var-app-name");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        CoordinatorJobBean job = checkCoordJobs(jobId);
        assertNotNull(job);
        assertEquals("var-app-name-foo", job.getAppName());
        assertEquals(job.getTimeout(), Services.get().getConf().getInt(
                "oozie.service.coord.normal.default.timeout", -2));
        assertEquals(job.getConcurrency(), Services.get().getConf().getInt(
                "oozie.service.coord.default.concurrency", 1));

    }

    public void testBasicSubmitWithDryRun() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"${appName}-foo\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" "
                + "end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>"
                + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>"
                + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template>  </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>"
                + "hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("appName", "var-app-name");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(true, conf);
        String dryrunStr = sc.call();
        String[] actions = dryrunStr.split("action for new instance");
        assertEquals(actions.length, 2);
    }

    public void testBasicSubmitWithStartTimeAfterEndTime() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2010-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);

        try {
            sc.call();
            fail("Expected to catch errors due to incorrectly specified Start and End Time");
        }
        catch (CommandException e) {
            assertEquals(sc.getJob().getStatus(), Job.Status.FAILED);
            assertEquals(e.getErrorCode(), ErrorCode.E1003);
            assertTrue(e.getMessage().contains("Coordinator Start Time must be earlier than End Time."));
        }
    }

    public void testBasicSubmitWithCronFrequency() throws Exception {
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"0 10 * * *\" start=\"2009-02-01T01:00Z\" "
                + "end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> "
                + "<execution>LIFO</execution> </controls>  <action> "
                + "<workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "</workflow> </action> </coordinator-app>";
        testBasicSubmitWithCronFrequency(appXml, true);
        appXml = "<coordinator-app name=\"NAME\" frequency=\"* * 30 FEB *\" start=\"2009-02-01T01:00Z\" "
                + "end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> "
                + "<execution>LIFO</execution> </controls>  <action> "
                + "<workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "</workflow> </action> </coordinator-app>";
        testBasicSubmitWithCronFrequency(appXml, false);
    }

    private void testBasicSubmitWithCronFrequency(String appXml, Boolean isValidFrequency) throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);

        if (isValidFrequency) {
            String jobId = sc.call();
            assertEquals(jobId.substring(jobId.length() - 2), "-C");
            CoordinatorJobBean job = (CoordinatorJobBean) sc.getJob();
            assertEquals(job.getTimeUnitStr(), "CRON");
        }
        else {
            try {
                String jobId = sc.call();
            }
            catch (Exception ex) {
                assertTrue(ex.getMessage().contains("Invalid coordinator cron frequency"));
            }
        }

    }

    public void testBasicSubmitWithIdenticalStartAndEndTime() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2010-02-01T01:00Z\" "
                + "end=\"2010-02-01T01:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template>  </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> "
                + "<app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);

        try {
            sc.call();
            fail("Expected to catch errors due to incorrectly specified Start and End Time");
        }
        catch (CommandException e) {
            assertEquals(sc.getJob().getStatus(), Job.Status.FAILED);
            assertEquals(e.getErrorCode(), ErrorCode.E1003);
            assertTrue(e.getMessage().contains("Coordinator Start Time must be earlier than End Time."));
        }
    }

    public void testBasicSubmitWithUnMaterializableFrequency() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"0/10 3 * * *\" start=\"2010-02-01T01:00Z\" "
                + "end=\"2010-02-01T02:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2010-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2010-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template>  </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> "
                + "<app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);

        try {
            sc.call();
            fail("Expected to catch errors due to bad combination of frequency and start and end time");
        }
        catch (CommandException e) {
            assertEquals(sc.getJob().getStatus(), Job.Status.FAILED);
            assertEquals(e.getErrorCode(), ErrorCode.E1003);
            assertTrue(e.getMessage().contains("materializes no actions between start and end time."));
        }
    }

    public void testBasicSubmitWithValidFrequency() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"0/10 1 * * *\" start=\"2010-02-01T01:00Z\" "
                + "end=\"2010-02-01T02:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}")
                + "</uri-template>  </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> "
                + "<app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);

        try {
            sc.call();
        }
        catch (CommandException e) {
            fail("Unexpected failure: " + e);
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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");

        // CASE 1: Failure case i.e. multiple data-in instances
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-input-instance1.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "coordinator.xml"));
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);

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
        writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf);

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
        writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf);

        try {
            sc.call();
        }
        catch (CommandException e) {
            fail("Unexpected failure: " + e);
        }

        // CASE 4: Success case i.e. Single instances for input and single instance for output, but both with ","
        reader = IOUtils.getResourceAsReader("coord-multiple-input-instance4.xml", -1);
        writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf);

        try {
            sc.call();
        }
        catch (CommandException e) {
            fail("Unexpected failure: " + e);
        }
    }

    /**
     * Testing for when user tries to submit a coordinator application having data-in events
     * that erroneously specify multiple input data instances inside a single <start-instance> tag.
     * Job gives submission error and indicates appropriate correction
     * Testing both negative(error) and well as positive(success) cases
     */
    public void testBasicSubmitWithMultipleStartInstancesInputEvent() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");

        // CASE 1: Failure case i.e. multiple data-in start-instances
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-input-start-instance1.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "coordinator.xml"));
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);

        try {
            sc.call();
            fail("Expected to catch errors due to incorrectly specified input data set start-instances");
        }
        catch (CommandException e) {
            assertEquals(sc.getJob().getStatus(), Job.Status.FAILED);
            assertEquals(e.getErrorCode(), ErrorCode.E1021);
            assertTrue(e.getMessage().contains(sc.COORD_INPUT_EVENTS)
                    && e.getMessage().contains("Coordinator app definition should not have multiple start-instances"));
        }

        // CASE 2: Success case i.e. Single start instances for input and single start instance for output, but both with ","
        reader = IOUtils.getResourceAsReader("coord-multiple-input-start-instance2.xml", -1);
        writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf);

        try {
            sc.call();
        }
        catch (CommandException e) {
            fail("Unexpected failure: " + e);
        }
    }

    /**
     * Testing for when user tries to submit a coordinator application having data-in events
     * that erroneously specify multiple input data instances inside a single <start-instance> tag.
     * Job gives submission error and indicates appropriate correction
     * Testing both negative(error) and well as positive(success) cases
     */
    public void testBasicSubmitWithMultipleEndInstancesInputEvent() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");

        // CASE 1: Failure case i.e. multiple data-in start-instances
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-input-end-instance1.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "coordinator.xml"));
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);

        try {
            sc.call();
            fail("Expected to catch errors due to incorrectly specified input data set end-instances");
        }
        catch (CommandException e) {
            assertEquals(sc.getJob().getStatus(), Job.Status.FAILED);
            assertEquals(e.getErrorCode(), ErrorCode.E1021);
            assertTrue(e.getMessage().contains(sc.COORD_INPUT_EVENTS)
                    && e.getMessage().contains("Coordinator app definition should not have multiple end-instances"));
        }

        // CASE 2: Success case i.e. Single end instances for input and single end instance for output, but both with ","
        reader = IOUtils.getResourceAsReader("coord-multiple-input-end-instance2.xml", -1);
        writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf);

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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");

        // CASE 1: Failure case i.e. multiple data-out instances
        Reader reader = IOUtils.getResourceAsReader("coord-multiple-output-instance1.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "coordinator.xml"));
        IOUtils.copyCharStream(reader, writer);

        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);

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
        writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf);

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
        writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf);

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
        writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        sc = new CoordSubmitXCommand(conf);

        try {
            sc.call();
        }
        catch (CommandException e) {
            fail("Not expected to fail here");
        }
    }

    public void testELdataIO_xsd_4() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";


        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        Reader reader = IOUtils.getResourceAsReader("coord-el-dataset-4.xml", -1);
        Writer writer = new FileWriter(new URI(appPath).getPath());

        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("nameNode", "hdfs://localhost:9000");
        conf.set("queueName", "default");
        conf.set("jobTracker", "localhost:9001");

        conf.set("examplesRoot", "examples");
        conf.set("aggregated_logs", "aggregated-logs");
        conf.set("raw_logs", "raw-logs");


        sc = new CoordSubmitXCommand(conf);

        try {
            sc.call();
        }
        catch (CommandException e) {
            e.printStackTrace();
            fail("should not throw exception " + e.getMessage());
        }
    }




    /**
     * Basic coordinator submit test with bundleId
     *
     * @throws Exception
     */
    public void testBasicSubmitWithBundleId() throws Exception {
        BundleJobBean coordJob = addRecordToBundleJobTable(Job.Status.PREP, false);
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());

        this.addRecordToBundleActionTable(coordJob.getId(), "COORD-NAME", 0, Job.Status.PREP);

        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, coordJob.getId(), "COORD-NAME");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        CoordinatorJobBean job = checkCoordJobs(jobId);
        if (job != null) {
            assertEquals(coordJob.getId(), job.getBundleId());
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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());

        this.addRecordToBundleActionTable("OOZIE-B", "COORD-NAME", 0, Job.Status.PREP);

        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf, "OOZIE-B", "COORD-NAME");
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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns='uri:oozie:coordinator:0.2' "
                +       "xmlns:sla='uri:oozie:sla:0.1'> <controls> <timeout>${coord:minutes(10)}</timeout> "
                +       "<concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "<dataset name=\"Stats\" frequency=\"${coord:days(1)}\" initial-instance=\"2009-01-01T01:00Z\" "
                + "timezone=\"UTC\"><uri-template>hcat://foo:11002/myOutputDatabase/myOutputTable/datestamp=${YEAR}${MONTH}${DAY}"
                + "</uri-template></dataset>"
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> "
                + "<data-out name=\"aggregated-logs\" dataset=\"Stats\">"
                + "<instance>${coord:current(0)}</instance></data-out>"
                + "</output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> "
                + " <sla:info>"
                + " <sla:app-name>test-app</sla:app-name>"
                + " <sla:nominal-time>${coord:nominalTime()}</sla:nominal-time>"
                + " <sla:should-start>${5 * MINUTES}</sla:should-start>"
                + " <sla:should-end>${ SLA_OFFSET * HOURS}</sla:should-end>"
                + " <sla:notification-msg>Notifying User for ${coord:nominalTime()}, ${coord:actualTime()},"
                + "${coord:formatTime(coord:nominalTime(),'yyyy-MM-dd')},${coord:dateOffset(coord:nominalTime(), 1, 'DAY')}"
                + "${coord:actionId()},${coord:name()}, ${coord:conf('nameNode')},${coord:user()},${coord:dataOut('LOCAL_A')}"
                + "${coord:databaseOut('aggregated-logs')},${coord:tableOut('aggregated-logs')},"
                + "${coord:dataOutPartitions('aggregated-logs')}"
                + "</sla:notification-msg>"
                + " <sla:alert-contact>abc@example.com</sla:alert-contact>"
                + " <sla:dev-contact>abc@example.com</sla:dev-contact>"
                + " <sla:qa-contact>abc@example.com</sla:qa-contact>"
                + " <sla:se-contact>abc@example.com</sla:se-contact>"
                + " <sla:alert-frequency>LAST_HOUR</sla:alert-frequency>"
                + " <sla:alert-percentage>10</sla:alert-percentage>" + "</sla:info>" + "</action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("SLA_OFFSET", "10");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        CoordinatorJobBean job = checkCoordJobs(jobId);
        assertEquals(job.getTimeout(), 10);
    }

    /**
     * Use fixed values for frequency
     *
     * @throws Exception
     */
    public void testSubmitFixedValues() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"60\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"120\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequencyERROR=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"60\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"120\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> "
                + "<controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> "
                + "<controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        // conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
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
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("MY_DONE_FLAG", "complete");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"${NAME}\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.3\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
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
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("MY_DONE_FLAG", "complete");
        conf.set("NAME", "test_app_name");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("MINUTES", "1");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);

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
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        Reader reader = IOUtils.getResourceAsReader("coord-dataset-initial-instance.xml", -1);
        Writer writer = new FileWriter(new File(getTestCaseDir(), "coordinator.xml"));
        IOUtils.copyCharStream(reader, writer);

        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
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

    /**
     * Basic submit with include file
     * @throws Exception
     */
    public void testBasicSubmitWithIncludeFile() throws Exception {
        Configuration conf = new XConfiguration();
        final File includePathFile = new File(getTestCaseDir(), "include1.xml");
        final String URI_TEMPLATE_INCLUDE_XML = "file:///tmp/include_xml/workflows/${YEAR}/${DAY}";
        final String URI_TEMPLATE_COORD_XML = "file:///tmp/coord_xml/workflows/${YEAR}/${DAY}";
        String includeXml =
            "<datasets> "
                + "<dataset name=\"A\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" timezone=\"UTC\">"
                    + "<uri-template>" + URI_TEMPLATE_INCLUDE_XML + "</uri-template>"
                + "</dataset> "
            + "</datasets>";
        writeToFile(includeXml, includePathFile);

        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml =
            "<coordinator-app name=\"${appName}-foo\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" "
                + "end=\"2009-02-03T23:59Z\" timezone=\"UTC\" xmlns=\"uri:oozie:coordinator:0.2\">"
            + "<controls> "
                + "<execution>LIFO</execution>"
            + "</controls>"
            + "<datasets> "
                + "<include>" + includePathFile.toURI() + "</include>"
                + "<dataset name=\"B\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" timezone=\"UTC\">"
                    + "<uri-template>" + URI_TEMPLATE_COORD_XML + "</uri-template>"
                + "</dataset> "
            + "</datasets>"
            + " <input-events> "
                + "<data-in name=\"inputA\" dataset=\"A\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "<data-in name=\"inputB\" dataset=\"B\"> <instance>${coord:latest(0)}</instance> </data-in>  "
            + "</input-events> "
            + "<action>"
                + "<workflow>"
                    + "<app-path>hdfs:///tmp/workflows/</app-path> "
                    + "<configuration>"
                        + "<property> <name>inputA</name> <value>${coord:dataIn('inputB')}</value> </property> "
                    + "</configuration>"
                + "</workflow>"
            + "</action>"
            + " </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("appName", "var-app-name");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        CoordinatorJobBean job = checkCoordJobs(jobId);
        assertNotNull(job);
        Element processedJobXml = XmlUtils.parseXml(job.getJobXml());

        Namespace namespace = processedJobXml.getNamespace();
        @SuppressWarnings("unchecked")
        List<Element> datainElements = processedJobXml.getChild("input-events", namespace).getChildren("data-in", namespace);
        assertTrue("<data-in> should be 2. One from coordinator.xml and the other from the include file"
                , datainElements.size() == 2);

        assertEquals(URI_TEMPLATE_INCLUDE_XML
                , datainElements.get(0).getChild("dataset", namespace).getChildText("uri-template", namespace));
        assertEquals(URI_TEMPLATE_COORD_XML
                , datainElements.get(1).getChild("dataset", namespace).getChildText("uri-template", namespace));
    }

    /**
     * https://issues.apache.org/jira/browse/OOZIE-1211
     * If a datasets include file has a dataset name as in one defined in coordinator.xml,
     * the one in coordinator.xml should be honored.
     * http://oozie.apache.org/docs/3.3.1/CoordinatorFunctionalSpec.html#a10.1.1._Dataset_Names_Collision_Resolution
     *
     * @throws Exception
     */
    public void testDuplicateDatasetNameInIncludeFile() throws Exception {
        Configuration conf = new XConfiguration();
        final File includePathFile = new File(getTestCaseDir(), "include1.xml");
        final String URI_TEMPLATE_INCLUDE_XML = "file:///tmp/include_xml/workflows/${YEAR}/${DAY}";
        final String URI_TEMPLATE_COORD_XML = "file:///tmp/coord_xml/workflows/${YEAR}/${DAY}";
        String includeXml =
            "<datasets> "
                + "<dataset name=\"B\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" timezone=\"UTC\">"
                    + "<uri-template>" + URI_TEMPLATE_INCLUDE_XML + "</uri-template>"
                + "</dataset> "
            + "</datasets>";
        writeToFile(includeXml, includePathFile);

        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml =
            "<coordinator-app name=\"${appName}-foo\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" "
                + "end=\"2009-02-03T23:59Z\" timezone=\"UTC\" xmlns=\"uri:oozie:coordinator:0.2\">"
            + "<controls> "
                + "<execution>LIFO</execution>"
            + "</controls>"
            + "<datasets> "
                + "<include>" + includePathFile.toURI() + "</include>"
                + "<dataset name=\"B\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" timezone=\"UTC\">"
                    + "<uri-template>" + URI_TEMPLATE_COORD_XML + "</uri-template>"
                + "</dataset> "
            + "</datasets>"
            + " <input-events> "
                + "<data-in name=\"inputB\" dataset=\"B\"> <instance>${coord:latest(0)}</instance> </data-in>  "
            + "</input-events> "
            + "<action>"
                + "<workflow>"
                    + "<app-path>hdfs:///tmp/workflows/</app-path> "
                    + "<configuration>"
                        + "<property> <name>inputB</name> <value>${coord:dataIn('inputB')}</value> </property> "
                    + "</configuration>"
                + "</workflow>"
            + "</action>"
            + " </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("appName", "var-app-name");
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        CoordinatorJobBean job = checkCoordJobs(jobId);
        assertNotNull(job);
        Element processedJobXml = XmlUtils.parseXml(job.getJobXml());

        Namespace namespace = processedJobXml.getNamespace();
        @SuppressWarnings("unchecked")
        List<Element> datasetElements = processedJobXml.getChild("input-events", namespace).getChild("data-in", namespace)
                .getChildren("dataset", namespace);
        assertTrue("<dataset> should not be duplicate", datasetElements.size() == 1);

        assertEquals(URI_TEMPLATE_COORD_XML, datasetElements.get(0).getChildText("uri-template", namespace));
        assertFalse("<uri-template> should not contain one from the include file"
                , job.getJobXml().contains(URI_TEMPLATE_INCLUDE_XML));
    }

    /**
     * Frequency faster/slower than than maximum
     *
     * @throws Exception
     */
    public void testCheckMaximumFrequency() throws Exception {
        assertTrue(Services.get().getConf().getBoolean("oozie.service.coord.check.maximum.frequency", false));
        _testCheckMaximumFrequencyHelper("5");
        _testCheckMaximumFrequencyHelper("10");
        _testCheckMaximumFrequencyHelper("${coord:hours(2)}");
        _testCheckMaximumFrequencyHelper("${coord:days(3)}");
        _testCheckMaximumFrequencyHelper("${coord:months(4)}");
        try {
            _testCheckMaximumFrequencyHelper("3");
            fail();
        } catch (CommandException ce) {
            assertEquals(ErrorCode.E1003, ce.getErrorCode());
            assertTrue(ce.getMessage().contains("Coordinator job with frequency [3] minutes is faster than allowed maximum of 5 "
                    + "minutes"));
        }
        try {
            Services.get().getConf().setBoolean("oozie.service.coord.check.maximum.frequency", false);
            _testCheckMaximumFrequencyHelper("5");
            _testCheckMaximumFrequencyHelper("10");
            _testCheckMaximumFrequencyHelper("${coord:hours(2)}");
            _testCheckMaximumFrequencyHelper("${coord:days(3)}");
            _testCheckMaximumFrequencyHelper("${coord:months(4)}");
            _testCheckMaximumFrequencyHelper("3");
        } finally {
            Services.get().getConf().setBoolean("oozie.service.coord.check.maximum.frequency", true);
        }
    }

    private void _testCheckMaximumFrequencyHelper(String freq) throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"" + freq + "\" start=\"2009-02-01T01:00Z\" "
                + "end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        checkCoordJobs(jobId);
    }

    private void _testConfigDefaults(boolean withDefaults) throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"${startTime}\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\"> <controls> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("coord/workflows/${YEAR}/${DAY}") + "</uri-template>  </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());

        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);

        if (withDefaults) {
            String defaults = "<configuration><property><name>startTime</name>" +
                              "<value>2009-02-01T01:00Z</value></property></configuration>";
            writeToFile(defaults, new File(getTestCaseDir(), CoordSubmitXCommand.CONFIG_DEFAULT));
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

    /**
     * Test timeout setting
     *
     * @throws Exception
     */
    public void testSubmitWithTimeout() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        // timeout unit = DAY
        String appXml1 = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" "
                + "start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns='uri:oozie:coordinator:0.2'> "
                + "<controls> <timeout>${coord:days(10)}</timeout> </controls> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> </workflow> "
                + "</action> </coordinator-app>";
        writeToFile(appXml1, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        CoordSubmitXCommand sc = new CoordSubmitXCommand(conf);
        String jobId = sc.call();
        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        CoordinatorJobBean job = checkCoordJobs(jobId);
        assertEquals(job.getTimeout(), 14400);
        // timeout unit = HOUR
        String appXml2 = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" "
                + "start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns='uri:oozie:coordinator:0.2'> "
                + "<controls> <timeout>${coord:hours(10)}</timeout> </controls> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> </workflow> "
                + "</action> </coordinator-app>";
        writeToFile(appXml2, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        sc = new CoordSubmitXCommand(conf);
        jobId = sc.call();
        job = checkCoordJobs(jobId);
        assertEquals(job.getTimeout(), 600);
        // timeout unit = MINUTE
        String appXml3 = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" "
                + "start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns='uri:oozie:coordinator:0.2'> "
                + "<controls> <timeout>${coord:minutes(10)}</timeout> </controls> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> </workflow> "
                + "</action> </coordinator-app>";
        writeToFile(appXml3, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        sc = new CoordSubmitXCommand(conf);
        jobId = sc.call();
        job = checkCoordJobs(jobId);
        assertEquals(job.getTimeout(), 10);
        // timeout unit = MONTH
        String appXml4 = "<coordinator-app name=\"NAME\" frequency=\"${coord:months(1)}\" "
                + "start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns='uri:oozie:coordinator:0.2'> "
                + "<controls> <timeout>${coord:months(1)}</timeout> </controls> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> </workflow> "
                + "</action> </coordinator-app>";
        writeToFile(appXml4, appPathFile);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        sc = new CoordSubmitXCommand(conf);
        jobId = sc.call();
        job = checkCoordJobs(jobId);
        assertEquals(job.getTimeout(), 43200);
    }

    public void testSubmitWithSLAAlertsDisable() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");

        // CASE 1: Failure case i.e. multiple data-in instances
        Reader reader = IOUtils.getResourceAsReader("coord-action-sla.xml", -1);
        Writer writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set("start", DateUtils.formatDateOozieTZ(new Date()));
        conf.set("end", DateUtils.formatDateOozieTZ(org.apache.commons.lang.time.DateUtils.addMonths(new Date(), 1)));
        conf.set("frequency", "coord:days(1)");
        conf.set(OozieClient.USER_NAME, getTestUser());
        reader = IOUtils.getResourceAsReader("wf-credentials.xml", -1);
        appPathFile = new File(getTestCaseDir(), "workflow.xml");
        writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        conf.set("wfAppPath", appPathFile.getPath());
        Date nominalTime = new Date();
        conf.set("nominal_time", DateUtils.formatDateOozieTZ(nominalTime));

        String coordId = new CoordSubmitXCommand(conf).call();
        new CoordMaterializeTransitionXCommand(coordId, 3600).call();
        SLAService slaService = services.get(SLAService.class);
        SLACalculator calc = slaService.getSLACalculator();
        SLACalcStatus slaCalc = calc.get(coordId + "@" + 1);
        assertFalse(Boolean.valueOf(slaCalc.getSLAConfigMap().get(OozieClient.SLA_DISABLE_ALERT)));

        Configuration conf1=new Configuration(conf);
        // CASE I: "ALL"
        conf1.set(OozieClient.SLA_DISABLE_ALERT, "ALL");
        coordId = new CoordSubmitXCommand(conf1).call();
        new CoordMaterializeTransitionXCommand(coordId, 3600).call();

        slaService = services.get(SLAService.class);
        calc = slaService.getSLACalculator();
        slaCalc = calc.get(coordId + "@" + 1);
        assertTrue(Boolean.valueOf(slaCalc.getSLAConfigMap().get(OozieClient.SLA_DISABLE_ALERT)));

        // CASE II: Date Range
        Configuration conf2=new Configuration(conf);
        Date startRangeDate = new Date(nominalTime.getTime() - 3600 * 1000);
        conf2.set(OozieClient.SLA_DISABLE_ALERT,
                DateUtils.formatDateOozieTZ(startRangeDate) + "::" + DateUtils.formatDateOozieTZ(nominalTime));
        coordId = new CoordSubmitXCommand(conf2).call();
        new CoordMaterializeTransitionXCommand(coordId, 3600).call();

        slaCalc = calc.get(coordId + "@" + 1);
        assertTrue(Boolean.valueOf(slaCalc.getSLAConfigMap().get(OozieClient.SLA_DISABLE_ALERT)));

        // CASE III: Coord name (negative test)
        Configuration conf3=new Configuration(conf);
        conf3.set(OozieClient.SLA_DISABLE_ALERT_COORD, "test-coord-sla-x");
        coordId = new CoordSubmitXCommand(conf3).call();
        new CoordMaterializeTransitionXCommand(coordId, 3600).call();
        slaCalc = calc.get(coordId + "@" + 1);
        assertFalse(Boolean.valueOf(slaCalc.getSLAConfigMap().get(OozieClient.SLA_DISABLE_ALERT)));

        // CASE IV: Older than n(hours)
        Date otherNominalTime = new Date(nominalTime.getTime() - 73 * 3600 * 1000);
        conf = new XConfiguration();
        appPathFile = new File(getTestCaseDir(), "coordinator.xml");
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set("wfAppPath", appPathFile.getPath());
        conf.set("start", DateUtils.formatDateOozieTZ(org.apache.commons.lang.time.DateUtils.addMonths(new Date(), -1)));
        conf.set("end", DateUtils.formatDateOozieTZ(org.apache.commons.lang.time.DateUtils.addMonths(new Date(), 1)));

        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("nominal_time", DateUtils.formatDateOozieTZ(otherNominalTime));
        conf.setInt(OozieClient.SLA_DISABLE_ALERT_OLDER_THAN, 72);
        coordId = new CoordSubmitXCommand(conf).call();
        new CoordMaterializeTransitionXCommand(coordId, 3600).call();
        slaCalc = calc.get(coordId + "@" + 1);
        assertTrue(Boolean.valueOf(slaCalc.getSLAConfigMap().get(OozieClient.SLA_DISABLE_ALERT)));

        // catchup mode
        conf = new XConfiguration();
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set("wfAppPath", appPathFile.getPath());
        conf.set("start", DateUtils.formatDateOozieTZ(org.apache.commons.lang.time.DateUtils.addMonths(new Date(), -1)));
        conf.set("end", DateUtils.formatDateOozieTZ(org.apache.commons.lang.time.DateUtils.addMonths(new Date(), 1)));

        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("nominal_time",
                DateUtils.formatDateOozieTZ(org.apache.commons.lang.time.DateUtils.addMonths(new Date(), -1)));
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("nominal_time",
                DateUtils.formatDateOozieTZ(org.apache.commons.lang.time.DateUtils.addMonths(new Date(), -1)));
        coordId = new CoordSubmitXCommand(conf).call();
        new CoordMaterializeTransitionXCommand(coordId, 3600).call();
        slaCalc = calc.get(coordId + "@" + 1);
        assertTrue(Boolean.valueOf(slaCalc.getSLAConfigMap().get(OozieClient.SLA_DISABLE_ALERT)));

        // normal mode
        conf = new XConfiguration();
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set("wfAppPath", appPathFile.getPath());
        conf.set("start", DateUtils.formatDateOozieTZ(new Date()));
        conf.set("end", DateUtils.formatDateOozieTZ(org.apache.commons.lang.time.DateUtils.addMonths(new Date(), 1)));

        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("nominal_time", DateUtils.formatDateOozieTZ(new Date()));
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("nominal_time", DateUtils.formatDateOozieTZ(new Date()));
        coordId = new CoordSubmitXCommand(conf).call();
        new CoordMaterializeTransitionXCommand(coordId, 3600).call();
        slaCalc = calc.get(coordId + "@" + 1);
        assertFalse(Boolean.valueOf(slaCalc.getSLAConfigMap().get(OozieClient.SLA_DISABLE_ALERT)));

    }

    public void testSLAAlertWithNewlyCreatedActions() throws Exception {
        Configuration conf = new XConfiguration();
        File appPathFile = new File(getTestCaseDir(), "coordinator.xml");

        // CASE 1: Failure case i.e. multiple data-in instances
        Reader reader = IOUtils.getResourceAsReader("coord-action-sla.xml", -1);
        Writer writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPathFile.toURI().toString());
        conf.set("start", DateUtils.formatDateOozieTZ(org.apache.commons.lang.time.DateUtils.addDays(new Date(), -1)));
        conf.set("end", DateUtils.formatDateOozieTZ(org.apache.commons.lang.time.DateUtils.addMonths(new Date(), 1)));
        conf.set(OozieClient.USER_NAME, getTestUser());
        reader = IOUtils.getResourceAsReader("wf-credentials.xml", -1);
        appPathFile = new File(getTestCaseDir(), "workflow.xml");
        writer = new FileWriter(appPathFile);
        IOUtils.copyCharStream(reader, writer);
        conf.set("wfAppPath", appPathFile.getPath());
        Date nominalTime = new Date();
        conf.set("nominal_time", DateUtils.formatDateOozieTZ(nominalTime));

        String coordId = new CoordSubmitXCommand(conf).call();
        CoordinatorJobBean job = CoordJobQueryExecutor.getInstance().get(
                CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB, coordId);
        job.setMatThrottling(1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, job);
        new CoordMaterializeTransitionXCommand(coordId, 3600).call();
        SLAService slaService = services.get(SLAService.class);
        SLACalculator calc = slaService.getSLACalculator();
        SLACalcStatus slaCalc = calc.get(coordId + "@" + 1);
        assertFalse(Boolean.valueOf(slaCalc.getSLAConfigMap().get(OozieClient.SLA_DISABLE_ALERT)));
        assertEquals(slaCalc.getExpectedDuration(), 1800000);
        job = CoordJobQueryExecutor.getInstance().get(CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB, coordId);
        assertEquals(job.getLastActionNumber(), 1);

        String newParams = RestConstants.SLA_MAX_DURATION + "=${5 * MINUTES}";

        new CoordSLAChangeXCommand(coordId, null, null, JobUtils.parseChangeValue(newParams)).call();
        new CoordSLAAlertsDisableXCommand(coordId, null, null).call();

        job = CoordJobQueryExecutor.getInstance().get(CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB, coordId);
        job.setMatThrottling(2);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQueryExecutor.CoordJobQuery.UPDATE_COORD_JOB, job);

        job = CoordJobQueryExecutor.getInstance().get(CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB, coordId);

        new CoordMaterializeTransitionXCommand(coordId, 3600).call();
        job = CoordJobQueryExecutor.getInstance().get(CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB, coordId);
        slaCalc = calc.get(coordId + "@" + job.getLastActionNumber());
        assertEquals(slaCalc.getExpectedDuration(), 300000);
        // newly action should have sla disable after coord disable command on coord job
        assertTrue(Boolean.valueOf(slaCalc.getSLAConfigMap().get(OozieClient.SLA_DISABLE_ALERT)));
        Element eAction = XmlUtils.parseXml(job.getJobXml());
        Element eSla = eAction.getChild("action", eAction.getNamespace()).getChild("info", eAction.getNamespace("sla"));
        assertEquals(SLAOperations.getTagElement(eSla, "max-duration"), "${5 * MINUTES}");
    }
}
