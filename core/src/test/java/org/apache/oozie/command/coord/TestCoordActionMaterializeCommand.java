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

import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.SLAStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordActionMaterializeCommand extends XTestCase {
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

    public void testActionMater() throws Exception {
        String jobId = "0000000-" + new Date().getTime() + "-testActionMater-C";

        Date startTime = DateUtils.parseDateUTC("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-11T10:00Z");
        addRecordToJobTable(jobId, startTime, endTime);
        new CoordActionMaterializeCommand(jobId, startTime, endTime).call();
        CoordinatorActionBean action = checkCoordAction(jobId + "@1");
    }

    public void testActionMaterWithPauseTime1() throws Exception {
        String jobId = "0000000-" + new Date().getTime() + "-testActionMater-C";

        Date startTime = DateUtils.parseDateUTC("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateUTC("2009-03-06T10:04Z");
        addRecordToJobTable(jobId, startTime, endTime, pauseTime);
        new CoordActionMaterializeCommand(jobId, startTime, endTime).call();
        checkCoordActions(jobId, 1, null);
    }

    public void testActionMaterWithPauseTime2() throws Exception {
        String jobId = "0000000-" + new Date().getTime() + "-testActionMater-C";

        Date startTime = DateUtils.parseDateUTC("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateUTC("2009-03-06T10:08Z");
        addRecordToJobTable(jobId, startTime, endTime, pauseTime);
        new CoordActionMaterializeCommand(jobId, startTime, endTime).call();
        checkCoordActions(jobId, 2, null);
    }

    public void testActionMaterWithPauseTime3() throws Exception {
        String jobId = "0000000-" + new Date().getTime() + "-testActionMater-C";

        Date startTime = DateUtils.parseDateUTC("2009-03-06T10:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-06T10:14Z");
        Date pauseTime = DateUtils.parseDateUTC("2009-03-06T09:58Z");
        addRecordToJobTable(jobId, startTime, endTime, pauseTime);
        new CoordActionMaterializeCommand(jobId, startTime, endTime).call();
        checkCoordActions(jobId, 0, CoordinatorJob.Status.RUNNING);
    }

    private void addRecordToJobTable(String jobId, Date startTime, Date endTime) throws StoreException {
        CoordinatorStore store = new CoordinatorStore(false);
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setStartTime(startTime);
        coordJob.setEndTime(endTime);
        coordJob.setTimeUnit(Timeunit.DAY);
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.PREMATER);
        coordJob.setCreatedTime(new Date()); // TODO: Do we need that?
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        coordJob.setTimeZone("America/Los_Angeles");
        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' xmlns:sla='uri:oozie:sla:0.1' name='NAME' frequency=\"1\" start='2009-03-06T010:00Z' end='2009-03-11T10:00Z' timezone='America/Los_Angeles' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<input-events>";
        appXml += "<data-in name='A' dataset='a'>";
        appXml += "<dataset name='a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${MONTH}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:current(0)}</instance>";
        appXml += "<instance>${coord:latest(-1)}</instance>";
        //appXml += "<start-instance>${coord:current(-2)}</start-instance>";
        //appXml += "<end-instance>${coord:current(0)}</end-instance>";
        appXml += "</data-in>";
        appXml += "</input-events>";
        appXml += "<output-events>";
        appXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        appXml += "<dataset name='local_a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:current(-1)}</instance>";
        appXml += "</data-out>";
        appXml += "</output-events>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>hdfs:///tmp/workflows/</app-path>";
        appXml += "<configuration>";
        appXml += "<property>";
        appXml += "<name>inputA</name>";
        appXml += "<value>${coord:dataIn('A')}</value>";
        appXml += "</property>";
        appXml += "<property>";
        appXml += "<name>inputB</name>";
        appXml += "<value>${coord:dataOut('LOCAL_A')}</value>";
        appXml += "</property>";
        appXml += "</configuration>";
        appXml += "</workflow>";
        appXml += " <sla:info>"
                // + " <sla:client-id>axonite-blue</sla:client-id>"
                + " <sla:app-name>test-app</sla:app-name>"
                + " <sla:nominal-time>${coord:nominalTime()}</sla:nominal-time>"
                + " <sla:should-start>5</sla:should-start>"
                + " <sla:should-end>120</sla:should-end>"
                + " <sla:notification-msg>Notifying User for ${coord:nominalTime()} nominal time </sla:notification-msg>"
                + " <sla:alert-contact>abc@yahoo.com</sla:alert-contact>"
                + " <sla:dev-contact>abc@yahoo.com</sla:dev-contact>"
                + " <sla:qa-contact>abc@yahoo.com</sla:qa-contact>" + " <sla:se-contact>abc@yahoo.com</sla:se-contact>"
                + "</sla:info>";
        appXml += "</action>";
        appXml += "</coordinator-app>";
        /*try {
            System.out.println(XmlUtils.prettyPrint(XmlUtils.parseXml(appXml)));
            ;
        }
        catch (JDOMException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }*/
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        try {
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-03-11T10:00Z"));
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail("Could not set end time");
        }
        try {
            store.beginTrx();
            store.insertCoordinatorJob(coordJob);
            store.commitTrx();
        }
        catch (StoreException se) {
            se.printStackTrace();
            store.rollbackTrx();
            fail("Unable to insert the test job record to table");
            throw se;
        }
        finally {
            store.closeTrx();
        }
    }

    private CoordinatorActionBean checkCoordAction(String actionId) throws StoreException {
        CoordinatorStore store = new CoordinatorStore(false);
        try {
            CoordinatorActionBean action = store.getCoordinatorAction(actionId, false);
            SLAStore slaStore = new SLAStore(store);
            long lastSeqId[] = new long[1];
            List<SLAEventBean> slaEvents = slaStore.getSLAEventListNewerSeqLimited(0, 10, lastSeqId);
            // System.out.println("AAA " + slaEvents.size() + " : " +
            // lastSeqId[0]);
            if (slaEvents.size() == 0) {
                fail("Unable to GET any record of sequence id greater than 0");
            }
            return action;
        }
        catch (StoreException se) {
            se.printStackTrace();
            fail("Action ID " + actionId + " was not stored properly in db");
        }
        return null;
    }

    private void addRecordToJobTable(String jobId, Date startTime, Date endTime, Date pauseTime) throws StoreException {
        CoordinatorStore store = new CoordinatorStore(false);
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setStartTime(startTime);
        coordJob.setEndTime(endTime);
        coordJob.setPauseTime(pauseTime);
        coordJob.setTimeUnit(Timeunit.MINUTE);
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.PREMATER);
        coordJob.setCreatedTime(new Date()); // TODO: Do we need that?
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        coordJob.setTimeZone("America/Los_Angeles");
        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' xmlns:sla='uri:oozie:sla:0.1' name='NAME' frequency=\"5\" start='2009-03-06T010:00Z' end='2009-03-06T10:14Z' timezone='America/Los_Angeles' freq_timeunit='MINUTE' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>hdfs:///tmp/workflows/</app-path>";
        appXml += "<configuration>";
        appXml += "</configuration>";
        appXml += "</workflow>";
        appXml += " <sla:info>"
                // + " <sla:client-id>axonite-blue</sla:client-id>"
                + " <sla:app-name>test-app</sla:app-name>"
                + " <sla:nominal-time>${coord:nominalTime()}</sla:nominal-time>"
                + " <sla:should-start>5</sla:should-start>"
                + " <sla:should-end>120</sla:should-end>"
                + " <sla:notification-msg>Notifying User for ${coord:nominalTime()} nominal time </sla:notification-msg>"
                + " <sla:alert-contact>abc@yahoo.com</sla:alert-contact>"
                + " <sla:dev-contact>abc@yahoo.com</sla:dev-contact>"
                + " <sla:qa-contact>abc@yahoo.com</sla:qa-contact>" + " <sla:se-contact>abc@yahoo.com</sla:se-contact>"
                + "</sla:info>";
        appXml += "</action>";
        appXml += "</coordinator-app>";

        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(5);
        try {
            store.beginTrx();
            store.insertCoordinatorJob(coordJob);
            store.commitTrx();
        }
        catch (StoreException se) {
            se.printStackTrace();
            store.rollbackTrx();
            fail("Unable to insert the test job record to table");
            throw se;
        }
        finally {
            store.closeTrx();
        }
    }

    private void checkCoordActions(String jobId, int number, CoordinatorJob.Status status) throws StoreException {
        CoordinatorStore store = new CoordinatorStore(false);
        try {
            int coordActionsCount = store.getActionsForCoordinatorJob(jobId, false);
            if (coordActionsCount != number) {
                fail("Should have " + number + " actions created for job " + jobId);
            }

            if (status != null) {
                CoordinatorJob job = store.getCoordinatorJob(jobId, false);
                if (job.getStatus() != status) {
                    fail("Job status " + job.getStatus() + " should be " + status);
                }
            }
        }
        catch (StoreException se) {
            se.printStackTrace();
            fail("Job ID " + jobId + " was not stored properly in db");
        }
    }
}
