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
package org.apache.oozie;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.CoordinatorJob.Status;
import org.apache.oozie.client.rest.JsonCoordinatorAction;
import org.apache.oozie.command.coord.CoordSubmitCommand;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.test.XTestCase.Predicate;
import org.apache.oozie.util.XConfiguration;

public class TestCoordinatorEngine extends XTestCase {
    private Services services;

    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testEngine() throws Exception {
        String appPath = getTestCaseDir();
        String jobId = _testSubmitJob(appPath);
        _testGetJob(jobId, appPath);
        _testGetJobs(jobId);
        _testStatus(jobId);
        _testGetDefinition(jobId);
        _testSubsetActions(jobId);
    }

    /**
     * Test Missing Dependencies with No Done Flag in Schema
     *
     * @throws Exception
     */
    public void testDoneFlag() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseDir();

        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-01T02:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(1)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file://" + getTestCaseDir() + "/workflows/${YEAR}/${DAY}</uri-template> "
                + "</dataset>"
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"local_a\"> <instance>${coord:current(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows2/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";

        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");

        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        final String jobId = ce.submitJob(conf, true);
        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                try {
                    List<CoordinatorAction> actions = ce.getCoordJob(jobId).getActions();
                    for (CoordinatorAction action : actions) {
                        CoordinatorAction.Status actionStatus = action.getStatus();
                        if (actionStatus == CoordinatorAction.Status.WAITING) {
                            return true;
                        }
                    }
                }
                catch (Exception ex) {
                    return false;
                }
                return false;
            }
        });

        List<CoordinatorAction> actions = ce.getCoordJob(jobId).getActions();
        assertTrue(actions.size() > 0);
        CoordinatorAction action = actions.get(0);
        String missingDeps = action.getMissingDependencies();
        System.out.println("Missing deps=" + missingDeps);
        //done flag is not added to the missing dependency list
        assertEquals("file://" + getTestCaseDir() + "/workflows/2009/01/_SUCCESS", missingDeps);
    }

    /**
     * Test Missing Dependencies with Done Flag in Schema
     *
     * @throws Exception
     */
    public void testCustomDoneFlag() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseDir();
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-01T02:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(1)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file://" + getTestCaseDir() + "/workflows/${YEAR}/${MONTH}/${DAY}</uri-template> "
                + "<done-flag>consume_me</done-flag> </dataset>"
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"local_a\"> <instance>${coord:current(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows2/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");

        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        final String jobId = ce.submitJob(conf, true);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                try {
                    List<CoordinatorAction> actions = ce.getCoordJob(jobId).getActions();
                    for (CoordinatorAction action : actions) {
                        CoordinatorAction.Status actionStatus = action.getStatus();
                        if (actionStatus == CoordinatorAction.Status.WAITING) {
                            return true;
                        }
                    }
                }
                catch (Exception ex) {
                    return false;
                }
                return false;
            }
        });


        List<CoordinatorAction> actions = ce.getCoordJob(jobId).getActions();
        assertTrue(actions.size() > 0);
        CoordinatorAction action = actions.get(0);
        String missingDeps = action.getMissingDependencies();
        System.out.println("..Missing deps=" + missingDeps);
        assertEquals("file://" + getTestCaseDir() + "/workflows/2009/02/01/consume_me", missingDeps);
    }


    /**
     * Test Missing Dependencies with Empty Done Flag in Schema
     *
     * @throws Exception
     */
    public void testEmptyDoneFlag() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseDir();
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-01T02:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(1)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file://" + getTestCaseDir() + "/workflows/${YEAR}/${MONTH}/${DAY}</uri-template> "
                + "<done-flag></done-flag> </dataset>"
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"local_a\"> <instance>${coord:current(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows2/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");

        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        final String jobId = ce.submitJob(conf, true);

        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                try {
                    List<CoordinatorAction> actions = ce.getCoordJob(jobId).getActions();
                    for (CoordinatorAction action : actions) {
                        CoordinatorAction.Status actionStatus = action.getStatus();
                        if (actionStatus == CoordinatorAction.Status.WAITING) {
                            return true;
                        }
                    }
                }
                catch (Exception ex) {
                    return false;
                }
                return false;
            }
        });


        List<CoordinatorAction> actions = ce.getCoordJob(jobId).getActions();
        assertTrue(actions.size() > 0);
        CoordinatorAction action = actions.get(0);
        String missingDeps = action.getMissingDependencies();
        System.out.println("..Missing deps=" + missingDeps);
        assertEquals("file://" + getTestCaseDir() + "/workflows/2009/02/01", missingDeps);
    }


    /**
     * Test Missing Dependencies with Done Flag in Schema
     *
     * @throws Exception
     */
    public void testDoneFlagCreation() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseDir();
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-01T02:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(1)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file://" + getTestCaseDir() + "/workflows/${YEAR}/${MONTH}/${DAY}</uri-template> "
                + "<done-flag>consume_me</done-flag> </dataset>"
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"local_a\"> <instance>${coord:current(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<action> <workflow> <app-path>hdfs:///tmp/workflows2/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "</configuration> </workflow> </action> </coordinator-app>";
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");

        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        final String jobId = ce.submitJob(conf, true);

        //create done flag
        String doneDir = getTestCaseDir() + "/workflows/2009/02/01";
        Process pr;
        try {
            pr = Runtime.getRuntime().exec("mkdir -p " + doneDir + "/consume_me");
            pr.waitFor();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        waitFor(10000, new Predicate() {
            public boolean evaluate() throws Exception {
                try {
                    List<CoordinatorAction> actions = ce.getCoordJob(jobId).getActions();
                    for (CoordinatorAction action : actions) {
                        CoordinatorAction.Status actionStatus = action.getStatus();
                        if (actionStatus == CoordinatorAction.Status.SUBMITTED) {
                            return true;
                        }
                    }
                }
                catch (Exception ex) {
                    return false;
                }
                return false;
            }
        });

        List<CoordinatorAction> actions = ce.getCoordJob(jobId).getActions();
        assertTrue(actions.size() > 0);
        CoordinatorAction action = actions.get(0);
        System.out.println("status=" + action.getStatus());
        String missingDeps = action.getMissingDependencies();
        System.out.println("..Missing deps=" + missingDeps);
        if (!(missingDeps == null || missingDeps.equals(""))) {
            fail();
        }
    }

    private String _testSubmitJob(String appPath) throws Exception {
        Configuration conf = new XConfiguration();

        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:minutes(20)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:minutes(20)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:minutes(20)}\" initial-instance=\"2009-02-01T01:00Z\" "
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
        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        final String jobId = ce.submitJob(conf, true);
        waitFor(5000, new Predicate() {
            public boolean evaluate() throws Exception {
                try {
                    ce.getJob(jobId).getStatus();
                }
                catch (Exception ex) {
                    return false;
                }
                return true;
            }
        });
        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        checkCoordJob(jobId);
        return jobId;
    }

    private void _testGetJob(String jobId, String appPath) throws Exception {
        CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        CoordinatorJob job = ce.getCoordJob(jobId);
        assertEquals(jobId, job.getId());
        assertEquals(job.getAppPath(), appPath);
    }

    public void _testGetJobs(String jobId) throws Exception {
        CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        CoordinatorJobInfo jobInfo = ce.getCoordJobs("", 1, 10); // TODO: use
        // valid
        // filter
        assertEquals(1, jobInfo.getCoordJobs().size());
        CoordinatorJob job = jobInfo.getCoordJobs().get(0);
        assertEquals(jobId, job.getId());
    }

    private void _testGetDefinition(String jobId) throws Exception {
        CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        CoordinatorJobBean job = ce.getCoordJob(jobId);
        System.out.println("JOBXML=" + job.getOrigJobXml());
        assertNotNull(job.getOrigJobXml());
    }

    /**
     * Helper methods
     *
     * @param jobId
     * @throws StoreException
     */
    private void checkCoordJob(String jobId) throws StoreException {
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        try {
            CoordinatorJobBean job = store.getCoordinatorJob(jobId, false);
        }
        catch (StoreException se) {
            se.printStackTrace();
            fail("Job ID " + jobId + " was not stored properly in db");
        }
    }

    private void writeToFile(String appXml, String appPath) throws IOException {
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

    private void _testStatus(final String jobId) throws Exception {
        waitFor(6000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
                CoordinatorJob job = ce.getCoordJob(jobId);
                return !job.getStatus().equals(CoordinatorJob.Status.PREP);
            }
        });

        CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        CoordinatorJob job = ce.getCoordJob(jobId);
        assertFalse(job.getStatus().equals(CoordinatorJob.Status.PREP));
    }

    private void _testSubsetActions(final String jobId) throws Exception {
        CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
        CoordinatorJob job = ce.getCoordJob(jobId, 1, 2);
        assertEquals(job.getActions().size(), 2);
    }
}
