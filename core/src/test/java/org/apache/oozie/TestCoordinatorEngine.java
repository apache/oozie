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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XConfiguration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.List;

public class TestCoordinatorEngine extends XTestCase {
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

    public void testEngine() throws Exception {
        String appPath = getTestCaseFileUri("coordinator.xml");
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
        String appPath = getTestCaseFileUri("coordinator.xml");

        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-01T02:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(1)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("workflows/${YEAR}/${DAY}") + "</uri-template> "
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


        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
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
        assertEquals(getTestCaseFileUri("workflows/2009/01/_SUCCESS"), missingDeps);
    }

    /**
     * Test Missing Dependencies with Done Flag in Schema
     *
     * @throws Exception
     */
    public void testCustomDoneFlag() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseFileUri("coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-01T02:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(1)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("workflows/${YEAR}/${MONTH}/${DAY}") + "</uri-template> "
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


        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
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
        assertEquals(new URI(getTestCaseFileUri("workflows/2009/02/01/consume_me")), new URI(missingDeps));
    }

    /**
     * Test Missing Dependencies with Empty Done Flag in Schema
     *
     * @throws Exception
     */
    public void testEmptyDoneFlag() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseFileUri("coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-01T02:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(1)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("workflows/${YEAR}/${MONTH}/${DAY}") + "</uri-template> "
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


        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
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
        assertEquals(getTestCaseFileUri("workflows/2009/02/01"), missingDeps);
    }

    /**
     * Test Missing Dependencies with Done Flag in Schema
     *
     * @throws Exception
     */
    public void testDoneFlagCreation() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseFileUri("coordinator.xml");
        String appXml = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-01T02:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(1)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("workflows/${YEAR}/${MONTH}/${DAY}") + "</uri-template> "
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


        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
        final String jobId = ce.submitJob(conf, true);

        //create done flag
        new File(getTestCaseDir(), "workflows/2009/02/01/consume_me").mkdirs();

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


        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
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
        CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
        CoordinatorJob job = ce.getCoordJob(jobId);
        assertEquals(jobId, job.getId());
        assertEquals(job.getAppPath(), appPath);
    }

    /**
     * Test to validate frequency and time unit filters for jobs
     *
     * @throws Exception
     */
    public void _testGetJobs(String jobId) throws Exception {
        CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
        // Test with no job filter specified
        CoordinatorJobInfo jobInfo = ce.getCoordJobs("", 1, 10);
        assertEquals(1, jobInfo.getCoordJobs().size());
        CoordinatorJob job = jobInfo.getCoordJobs().get(0);
        assertEquals(jobId, job.getId());

        // Test specifying the value for unit but leaving out the value for frequency
        try {
            jobInfo = ce.getCoordJobs("unit=minutes", 1, 10);
        }
        catch (CoordinatorEngineException ex) {
            assertEquals("E0420: Invalid jobs filter [unit=minutes], time unit should be added only when "
                    + "frequency is specified. Either specify frequency also or else remove the time unit", ex
                    .getMessage());
        }

        // Test for invalid frequency value(Non-numeric value)
        try {
            jobInfo = ce.getCoordJobs("frequency=ghj;unit=minutes", 1, 10);
        }
        catch (CoordinatorEngineException ex) {
            assertEquals("E0420: Invalid jobs filter [frequency=ghj;unit=minutes], "
                    + "invalid value [ghj] for frequency. A numerical value is expected", ex.getMessage());
        }

        // Test for invalid unit value(Other than months, days, minutes or hours)
        try {
            jobInfo = ce.getCoordJobs("frequency=60;unit=min", 1, 10);
        }
        catch (CoordinatorEngineException ex) {
            assertEquals("E0420: Invalid jobs filter [frequency=60;unit=min], invalid value [min] for time unit. "
                    + "Valid value is one of months, days, hours or minutes", ex.getMessage());
        }
    }

    private void _testGetDefinition(String jobId) throws Exception {
        CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
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
        try {
            CoordinatorJobBean job = CoordJobQueryExecutor.getInstance()
                    .get(CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB, jobId);
        }
        catch (JPAExecutorException se) {
            se.printStackTrace();
            fail("Job ID " + jobId + " was not stored properly in db");
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

    private void _testStatus(final String jobId) throws Exception {
        waitFor(6000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
                CoordinatorJob job = ce.getCoordJob(jobId);
                return !job.getStatus().equals(CoordinatorJob.Status.PREP);
            }
        });

        CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
        CoordinatorJob job = ce.getCoordJob(jobId);
        assertFalse(job.getStatus().equals(CoordinatorJob.Status.PREP));
    }

    private void _testSubsetActions(final String jobId) throws Exception {
        CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
        // Check for WAITING filter
        CoordinatorJob job = ce.getCoordJob(jobId, "status=WAITING", 1, 2, false);
        // As both actions are waiting, expected result size is 2
        assertEquals(job.getActions().size(), 2);

        job = ce.getCoordJob(jobId, "status=WAITING", 1, 0, false);
        // Since length is 0, number of actions returned should be 0.
        assertEquals(job.getActions().size(), 0);

        job = ce.getCoordJob(jobId, "status=RUNNING", 1, 2, false);
        assertEquals(job.getActions().size(), 0);

        //Check for actions WAITING OR RUNNING
        job = ce.getCoordJob(jobId, "status=RUNNING;status=WAITING", 1, 2, false);
        assertEquals(job.getActions().size(), 2);

        //Check without filters
        job = ce.getCoordJob(jobId, null, 1, 2, false);
        assertEquals(job.getActions().size(), 2);

        //Check for empty filter list
        job = ce.getCoordJob(jobId, "", 1, 2, false);
        assertEquals(job.getActions().size(), 2);

        //Check for negative filter
        job = ce.getCoordJob(jobId, "status!=RUNNING", 1, 2, false);
        assertEquals(job.getActions().size(), 2);

        //Check for multiple negative filter
        job = ce.getCoordJob(jobId, "status!=RUNNING;status!=WAITING", 1, 2, false);
        assertEquals(job.getActions().size(), 0);

        //Check for combination of positive and negative filter
        try {
            job = ce.getCoordJob(jobId, "status=WAITING;status!=WAITING", 1, 2, false);
        }
        catch (CoordinatorEngineException ex) {
            assertEquals(ErrorCode.E0421, ex.getErrorCode());
            assertEquals(
                    "E0421: Invalid job filter [status=WAITING;status!=WAITING], the status [WAITING] "
                    + "specified in both positive and negative filters",
                    ex.getMessage());
        }

        //Check for missing "="
        try {
            job = ce.getCoordJob(jobId, "statusRUNNING", 1, 2, false);
        }
        catch (CoordinatorEngineException ex) {
            assertEquals(ErrorCode.E0421, ex.getErrorCode());
            assertEquals("E0421: Invalid job filter [statusRUNNING], " +
                    "filter should be of format <key><comparator><value> pairs", ex.getMessage());
        }

        //Check for missing value after "="
        try {
            job = ce.getCoordJob(jobId, "status=", 1, 2, false);
        }
        catch (CoordinatorEngineException ex) {
            assertEquals(ErrorCode.E0421, ex.getErrorCode());
            assertEquals("E0421: Invalid job filter [status=], invalid status value []. Valid status values are: ["
                + StringUtils.join(CoordinatorAction.Status.values(), ", ") + "]", ex.getMessage());
        }

        // Check for invalid status value
        try {
            job = ce.getCoordJob(jobId, "status=blahblah", 1, 2, false);
        }
        catch (CoordinatorEngineException ex) {
            assertEquals(ErrorCode.E0421, ex.getErrorCode());
            assertEquals("E0421: Invalid job filter [status=blahblah], invalid status value [blahblah]."
                + " Valid status values are: ["
                + StringUtils.join(CoordinatorAction.Status.values(), ", ") + "]", ex.getMessage());
        }

        // Check for empty status value
        try {
            job = ce.getCoordJob(jobId, "status=\"\"", 1, 2, false);
        }
        catch (CoordinatorEngineException ex) {
            assertEquals(ErrorCode.E0421, ex.getErrorCode());
            assertEquals("E0421: Invalid job filter [status=\"\"], invalid status value [\"\"]."
                + " Valid status values are: ["
                + StringUtils.join(CoordinatorAction.Status.values(), ", ") + "]", ex.getMessage());
        }

        // Check for invalid filter option
        try {
            job = ce.getCoordJob(jobId, "blahblah=blahblah", 1, 2, false);
        }
        catch (CoordinatorEngineException ex) {
            assertEquals(ErrorCode.E0421, ex.getErrorCode());
            assertEquals("E0421: Invalid job filter [blahblah=blahblah], invalid filter [blahblah]. " +
                "Valid filters [" + StringUtils.join(CoordinatorEngine.VALID_JOB_FILTERS, ", ") + "]", ex.getMessage());
        }
    }
}
