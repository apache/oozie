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
package org.apache.oozie.store;

import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.service.CoordinatorStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;

public class TestCoordinatorStore extends XTestCase {
    Services services;
    CoordinatorStore store;
    CoordinatorJobBean coordBean;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        cleanUpDB(services.getConf());
        services.init();
        store = Services.get().get(CoordinatorStoreService.class).create();
    }

    @Override
    protected void tearDown() throws Exception {
        // dropSchema(dbName, conn);
        services.destroy();
        super.tearDown();
    }

    public void testCoordStore() throws StoreException {
        String jobId = "00000-" + new Date().getTime() + "-TestCoordinatorStore-C";
        String actionId = jobId + "_1";
        try {
            _testInsertJob(jobId);
            _testGetJob(jobId);
            _testGetMatJobLists();
            _testUpdateCoordJob(jobId);
            _testInsertAction(jobId, actionId);
            _testGetAction(jobId, actionId);
            _testGetActionForJob(jobId, actionId);
            _testGetActionForJobInExecOrder(jobId, actionId);
            _testGetActionForJobInLastOnly(jobId, actionId);
            _testGetActionRunningCount(actionId);
            _testGetRecoveryActionsGroupByJobId(jobId);
            _testUpdateCoordAction(actionId);
            _testUpdateCoordActionMin(actionId);
        }
        finally {
            // store.closeTrx();
        }
    }

    private void _testUpdateCoordAction(String actionId) {
        store.beginTrx();
        try {
            CoordinatorActionBean action = store.getCoordinatorAction(actionId, true);
            int newActNum = action.getActionNumber() + 1;
            action.setActionNumber(newActNum);
            store.updateCoordinatorAction(action);
            store.getEntityManager().flush();
            store.getEntityManager().merge(action);
            action = store.getCoordinatorAction(actionId, false);
            assertEquals(newActNum, action.getActionNumber());
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to Update a record in Coord Action. actionId =" + actionId);
        }

    }

    private void _testUpdateCoordActionMin(String actionId) {
        store.beginTrx();
        try {
            CoordinatorActionBean action = store.getCoordinatorAction(actionId, true);
            action.setStatus(CoordinatorAction.Status.SUCCEEDED);
            action.setMissingDependencies("d1,d2,d3");
            action.setActionNumber(777);
            Date lastModifiedTime = new Date();
            action.setLastModifiedTime(lastModifiedTime);
            store.updateCoordActionMin(action);
            store.commitTrx();
            //store.getEntityManager().merge(action);
            action = getCoordAction(actionId);
            assertEquals(CoordinatorAction.Status.SUCCEEDED, action.getStatus());
            assertEquals("d1,d2,d3", action.getMissingDependencies());
            //assertEquals(lastModifiedTime, action.getLastModifiedTime());
            if (action.getActionNumber() == 777) {
                fail("Action number should not be updated");
            }
        }
        catch (Exception ex) {
            if (store.isActive()) {
                store.rollbackTrx();
            }
            ex.printStackTrace();
            fail("Unable to Update a record in Coord Action. actionId =" + actionId);
        }

    }

    private void _testGetActionRunningCount(String actionId) {
        store.beginTrx();
        try {
            int count = store.getCoordinatorRunningActionsCount(actionId);
            assertEquals(count, 0);
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to GET count for action ID. actionId =" + actionId);
        }
    }


    private void _testGetActionForJobInExecOrder(String jobId, String actionId) {
        store.beginTrx();
        try {
            List<CoordinatorActionBean> actionList = store.getCoordinatorActionsForJob(jobId, 1,
                                                                                       CoordinatorJob.Execution.FIFO.toString());
            assertEquals(actionList.size(), 1);
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to GET a record for COORD Action_FOR_JOB with Exec Order. actionId =" + actionId + " jobId ="
                    + jobId);
        }
    }

    private void _testGetActionForJobInLastOnly(String jobId, String actionId) {
        store.beginTrx();
        try {
            List<CoordinatorActionBean> actionList = store.getCoordinatorActionsForJob(jobId, 3,
                                                                                       CoordinatorJob.Execution.LAST_ONLY.toString());
            assertEquals(actionList.size(), 1);
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to GET a record for COORD Action_FOR_JOB with Exec Order. actionId =" + actionId + " jobId ="
                    + jobId);
        }
    }

    private void _testGetActionForJob(String jobId, String actionId) {
        store.beginTrx();
        try {
            int coordActionsCount = store.getActionsForCoordinatorJob(jobId, false);
            assertEquals(coordActionsCount, 1);
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to GET a record for COORD Action_FOR_JOB. actionId =" + actionId + " jobId =" + jobId);
        }
    }

    private void _testGetAction(String jobId, String actionId) throws StoreException {
        store.beginTrx();
        try {
            CoordinatorActionBean action = store.getCoordinatorAction(actionId, false);
            assertEquals(jobId, action.getJobId());
            assertEquals(action.getStatus(), CoordinatorAction.Status.READY);
            assertEquals(action.getActionNumber(), 1);
            assertEquals(action.getExternalId(), actionId + "_E");
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to GET a record for COORD Action. actionId =" + actionId);
        }
    }

    private void _testGetRecoveryActionsGroupByJobId(String jobId) throws StoreException {
        store.beginTrx();
        try {
            List<String> jobids = store.getRecoveryActionsGroupByJobId(60);
            assertNotNull(jobids);
            assertEquals(jobId, jobids.get(0));
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to GET a record for RecoveryActionsGroupByJobId. jobId =" + jobId);
        }
    }

    private void _testInsertAction(String jobId, String actionId) {
        CoordinatorActionBean action = createAction(jobId, actionId);
    }

    private CoordinatorActionBean createAction(String jobId, String actionId) {
        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setJobId(jobId);
        action.setId(actionId);
        action.setActionNumber(1);
        action.setNominalTime(new Date());
        action.setStatus(Status.READY);
        action.setExternalId(actionId + "_E");
        action.setLastModifiedTime(new Date(new Date().getTime() - 1200000));
        store.beginTrx();
        try {
            store.insertCoordinatorAction(action);
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to insert a record into COORD Action ");
        }
        return action;
    }

    private void _testUpdateCoordJob(String jobId) {
        store.beginTrx();
        try {
            CoordinatorJobBean job = store.getCoordinatorJob(jobId, false);
            int newFreq = job.getFrequency() + 1;
            job.setFrequency(newFreq);
            store.updateCoordinatorJob(job);
            store.getEntityManager().flush();
            store.getEntityManager().merge(job);
            job = store.getCoordinatorJob(jobId, false);
            assertEquals(newFreq, job.getFrequency());
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to UPDATE a record for COORD Job. jobId =" + jobId);
        }

    }

    private void _testGetMatJobLists() throws StoreException {
        store.beginTrx();
        try {
            Date d1 = new Date();
            Date d2 = new Date(d1.getTime() + 1000);
            List<CoordinatorJobBean> jobList = store.getCoordinatorJobsToBeMaterialized(d2, 50);
            if (jobList.size() == 0) {
                fail("Test of getCoordinatorJobsToBeMaterialized returned no records. Date =" + d2);
            }
            // Assumption: no other older records are there
            d2 = new Date(d1.getTime() - 86400000L * 365L);
            jobList = store.getCoordinatorJobsToBeMaterialized(d2, 50);
            /*
             * if(jobList.size() > 0){ fail("Test of
             * getCoordinatorJobsToBeMaterialized returned some records while
             * expecting no records = " + d2); }
             */
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to Get Materialized Jobs ");
        }
    }

    private void _testGetJob(String jobId) throws StoreException {
        store.beginTrx();
        try {
            CoordinatorJobBean job = store.getCoordinatorJob(jobId, false);
            assertEquals(jobId, job.getId());
            assertEquals(job.getStatus(), CoordinatorJob.Status.PREP);
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to GET a record for COORD Job. jobId =" + jobId);
        }
    }

    private void _testInsertJob(String jobId) throws StoreException {
        CoordinatorJobBean job = createCoordJob(jobId);
        store.beginTrx();
        try {
            store.insertCoordinatorJob(job);
            store.commitTrx();
        }
        catch (Exception ex) {
            store.rollbackTrx();
            ex.printStackTrace();
            fail("Unable to insert a record into COORD Job ");
        }
    }

    private CoordinatorJobBean createCoordJob(String jobId) {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();

        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.PREP);
        coordJob.setCreatedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' name='NAME' frequency=\"1\" start='2009-02-01T01:00Z' end='2009-02-03T23:59Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<input-events>";
        appXml += "<data-in name='A' dataset='a'>";
        appXml += "<dataset name='a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coord:latest(0)}</instance>";
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
        appXml += "</action>";
        appXml += "</coordinator-app>";
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        Date curr = new Date();
        coordJob.setNextMaterializedTime(curr);
        coordJob.setLastModifiedTime(curr);
        coordJob.setEndTime(new Date(curr.getTime() + 86400000));
        coordJob.setStartTime(new Date(curr.getTime() - 86400000));
        return coordJob;
    }

    /**
     * Helper methods
     *
     * @param jobId
     * @throws StoreException
     */
    private CoordinatorActionBean getCoordAction(String actionId) throws StoreException {
        CoordinatorStore store = new CoordinatorStore(false);
        try {
            CoordinatorActionBean action = store.getCoordinatorAction(actionId, false);
            return action;
        }
        catch (StoreException se) {
            fail("Job ID " + actionId + " was not stored properly in db");
        }finally {
            store.closeTrx();
        }
        return null;
    }

}
