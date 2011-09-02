/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.coord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordSubmitCommand;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.test.XTestCase.Predicate;
import org.apache.oozie.util.XConfiguration;

public class TestCoordSubmitCommand extends XTestCase {
    private Services services;

    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

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
        String appPath = getTestCaseDir();
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
        conf.set(OozieClient.GROUP_NAME, "other");
        CoordSubmitCommand sc = new CoordSubmitCommand(conf, "UNIT_TESTING");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        CoordinatorJobBean job = checkCoordJobs(jobId);
        if (job != null) {
            assertEquals(job.getTimeout(), Services.get().getConf().getInt(
                    "oozie.service.coord.normal.default.timeout", -2));
        }
    }

    /**
     * Basic test
     *
     * @throws Exception
     */
    public void testBasicSubmitWithSLA() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseDir();
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns='uri:oozie:coordinator:0.1' xmlns:sla='uri:oozie:sla:0.1'> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
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
        // /System.out.println("MMMMM\n"+ appXml);
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");
        CoordSubmitCommand sc = new CoordSubmitCommand(conf, "UNIT_TESTING");
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
        String appPath = getTestCaseDir();
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
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
        conf.set(OozieClient.GROUP_NAME, "other");
        CoordSubmitCommand sc = new CoordSubmitCommand(conf, "UNIT_TESTING");
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
        String appPath = getTestCaseDir();
        String appXml = "<coordinator-app name=\"NAME\" frequencyERROR=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
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
        conf.set(OozieClient.GROUP_NAME, "other");
        CoordSubmitCommand sc = new CoordSubmitCommand(conf, "UNIT_TESTING");
        String jobId = null;
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
        String appPath = getTestCaseDir();
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> "
                + "<controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");
        CoordSubmitCommand sc = new CoordSubmitCommand(conf, "UNIT_TESTING");
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
        String appPath = getTestCaseDir();
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> "
                + "<controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        // conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");
        CoordSubmitCommand sc = new CoordSubmitCommand(conf, "UNIT_TESTING");
        String jobId = null;
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
        String appPath = getTestCaseDir();
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");
        CoordSubmitCommand sc = new CoordSubmitCommand(conf, "UNIT_TESTING");
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
        String appPath = getTestCaseDir();
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
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
        conf.set(OozieClient.GROUP_NAME, "other");
        conf.set("MY_DONE_FLAG", "complete");
        CoordSubmitCommand sc = new CoordSubmitCommand(conf, "UNIT_TESTING");
        String jobId = sc.call();

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        checkCoordJobs(jobId);
    }

    /**
     * Don't include controls in XML.
     *
     * @throws Exception
     */
    public void testSubmitReservedVars() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseDir();
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"10\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>blah</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");
        conf.set("MINUTES", "1");
        CoordSubmitCommand sc = new CoordSubmitCommand(conf, "UNIT_TESTING");

        try {
            sc.call();
            fail("Coord job submission should fail with reserved variable definitions.");
        }
        catch (CommandException ce) {

        }
    }

    /**
     * Helper methods
     *
     * @param jobId
     * @throws StoreException
     */
    private CoordinatorJobBean checkCoordJobs(String jobId) throws StoreException {
        CoordinatorStore store = new CoordinatorStore(false);
        try {
            CoordinatorJobBean job = store.getCoordinatorJob(jobId, false);
            return job;
        }
        catch (StoreException se) {
            fail("Job ID " + jobId + " was not stored properly in db");
        }
        return null;
    }

    private void writeToFile(String appXml, String appPath) throws IOException {
        // TODO Auto-generated method stub
        File wf = new File(appPath + "/coordinator.xml");
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
