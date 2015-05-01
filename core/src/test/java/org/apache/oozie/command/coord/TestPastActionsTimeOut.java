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
import java.net.URL;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XConfiguration;

public class TestPastActionsTimeOut extends XTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testEngine() throws Exception {
        String appPath = getTestCaseFileUri("coordinator.xml");
        String jobId = _testSubmitJob(appPath);
        _testTimeout(jobId);
    }

    private String _testSubmitJob(String appPath) throws Exception {
        Configuration conf = new XConfiguration();

        String appXml = "<coordinator-app name=\"NAME\" frequency=\"15\" start=\"2009-02-01T01:00Z\" end=\"2009-02-01T02:00Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("${YEAR}/${DAY}") + "</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>" + getTestCaseFileUri("${YEAR}/${DAY}") + "</uri-template> </dataset> "
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

        CoordinatorEngine ce = new CoordinatorEngine(getTestUser());
        String jobId = ce.submitJob(conf, true);

        assertEquals(jobId.substring(jobId.length() - 2), "-C");
        checkCoordJob(jobId);
        return jobId;
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
            fail("Job ID " + jobId + " was not stored properly in db");
        }
    }

    private void writeToFile(String appXml, String appPath) throws Exception {
        // TODO Auto-generated method stub
        File wf = new File(new URL(appPath).getPath());
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

    /**
     * The catch-up mode time up has been setup in {@link CoordActionMaterializeXCommand}
     * @param jobId job id
     * @throws Exception thrown if failed
     */
    private void _testTimeout(final String jobId) throws Exception {
        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser());

        waitFor(6000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJob job = ce.getCoordJob(jobId);
                return !(job.getStatus().equals(CoordinatorJob.Status.PREP));
            }
        });

        CoordinatorJob job = ce.getCoordJob(jobId);
        assertTrue(!(job.getStatus().equals(CoordinatorJob.Status.PREP)));

        waitFor(6000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJob job = ce.getCoordJob(jobId);
                List<CoordinatorAction> actions = job.getActions();
                return actions.size() > 0;
            }
        });

        job = ce.getCoordJob(jobId);
        List<CoordinatorAction> actions = job.getActions();
        assertTrue(actions.size() > 0);

        for (CoordinatorAction action : actions) {
            CoordinatorActionBean json = (CoordinatorActionBean) action;
            assertEquals(10, json.getTimeOut());
        }
    }
}
