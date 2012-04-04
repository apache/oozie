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
import java.net.URI;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.rest.JsonCoordinatorAction;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XConfiguration;

public class TestFutureActionsTimeOut extends XTestCase {
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
        String appPath = "file://" + getTestCaseDir() + File.separator + "coordinator.xml";
        String jobId = _testSubmitJob(appPath);
        Date createDate = new Date();
        _testTimeout(jobId, createDate);
    }

    private String _testSubmitJob(String appPath) throws Exception {
        Configuration conf = new XConfiguration();

        GregorianCalendar start = new GregorianCalendar(TimeZone
                .getTimeZone("GMT"));
        start.add(Calendar.MINUTE, -15);

        GregorianCalendar end = new GregorianCalendar(TimeZone
                .getTimeZone("GMT"));
        end.add(Calendar.MINUTE, 45);

        String appXml = "<coordinator-app name=\"NAME\" frequency=\"5\" "
                + "start=\""
                + start.get(Calendar.YEAR)
                + "-"
                + (start.get(Calendar.MONTH) + 1)
                + "-"
                + start.get(Calendar.DAY_OF_MONTH)
                + "T"
                + start.get(Calendar.HOUR_OF_DAY)
                + ":"
                + start.get(Calendar.MINUTE)
                + "Z\" "
                + "end=\""
                + end.get(Calendar.YEAR)
                + "-"
                + (end.get(Calendar.MONTH) + 1)
                + "-"
                + end.get(Calendar.DAY_OF_MONTH)
                + "T"
                + end.get(Calendar.HOUR_OF_DAY)
                + ":"
                + end.get(Calendar.MINUTE)
                + "Z\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.1\"> <controls> <timeout>10</timeout> <concurrency>2</concurrency> "
                + "<execution>LIFO</execution> </controls> <datasets> "
                + "<dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"9999-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "<dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"9999-02-01T01:00Z\" "
                + "timezone=\"UTC\"> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> "
                + "</datasets> <input-events> "
                + "<data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in>  "
                + "</input-events> "
                + "<output-events> <data-out name=\"LOCAL_A\" dataset=\"local_a\"> "
                + "<instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> "
                + "<configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> "
                + "<property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> "
                + "</property></configuration> </workflow> </action> </coordinator-app>";
        System.out.println(appXml);
        writeToFile(appXml, appPath);
        conf.set(OozieClient.COORDINATOR_APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());

        CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");
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
        CoordinatorStore store = Services.get().get(StoreService.class)
                .getStore(CoordinatorStore.class);
        try {
            CoordinatorJobBean job = store.getCoordinatorJob(jobId, false);
        }
        catch (StoreException se) {
            fail("Job ID " + jobId + " was not stored properly in db");
        }
    }

    private void writeToFile(String appXml, String appPath) throws Exception {
        // TODO Auto-generated method stub
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

    /**
     * The catch-up mode time up has been setup in {@link CoordActionMaterializeXCommand}
     * @param jobId job id
     * @param createDate create date
     * @throws Exception thrown if failed
     */
    private void _testTimeout(final String jobId, Date createDate) throws Exception {
        final CoordinatorEngine ce = new CoordinatorEngine(getTestUser(), "UNIT_TESTING");

        waitFor(12000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJob job = ce.getCoordJob(jobId);
                return !(job.getStatus().equals(CoordinatorJob.Status.PREP));
            }
        });

        CoordinatorJob job = ce.getCoordJob(jobId);
        assertTrue(!(job.getStatus().equals(CoordinatorJob.Status.PREP)));

        waitFor(12000, new Predicate() {
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
            JsonCoordinatorAction jsonAction = (JsonCoordinatorAction) action;

            if (jsonAction.getNominalTime().before(createDate)) {
                assertEquals(10, jsonAction.getTimeOut());
            }
            else {
                assertEquals(10, jsonAction.getTimeOut());
            }
        }
    }
}
