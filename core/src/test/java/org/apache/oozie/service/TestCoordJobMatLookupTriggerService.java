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
package org.apache.oozie.service;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.executor.jpa.CoordJobGetRunningActionsCountJPAExecutor;
import org.apache.oozie.service.CoordJobMatLookupTriggerService.CoordJobMatLookupTriggerRunnable;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;

public class TestCoordJobMatLookupTriggerService extends XFsTestCase {
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
     * Tests functionality of the CoordJobMatLookupTriggerService Runnable
     * command. </p> Insert a coordinator job with PREP. Then, runs the
     * CoordJobMatLookupTriggerService runnable and ensures the job status
     * changes to PREMATER.
     *
     * @throws Exception
     */
    public void testCoordJobMatLookupTriggerService1() throws Exception {
        final String jobId = "0000000-" + new Date().getTime() + "-testCoordRecoveryService-C";
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        Date start = DateUtils.parseDateUTC("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateUTC("2009-02-03T23:59Z");
        addRecordToJobTable(jobId, store, start, end);
        store.commitTrx();
        store.closeTrx();

        Thread.sleep(3000);
        Runnable runnable = new CoordJobMatLookupTriggerRunnable(3600);
        runnable.run();
        Thread.sleep(6000);
        
        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorJobBean coordJob = store2.getCoordinatorJob(jobId, false);
        store2.commitTrx();
        store2.closeTrx();
        if (!(coordJob.getStatus() == CoordinatorJob.Status.PREMATER)) {
            fail();
        }
        JPAService jpaService = Services.get().get(JPAService.class);
        int numWaitingActions = jpaService.execute(new CoordJobGetRunningActionsCountJPAExecutor(coordJob.getId()));
        assert(numWaitingActions <= coordJob.getConcurrency());
    }

    private String getCoordJobXml(Path appPath, Date start, Date end) throws Exception {
        String startDateStr = null, endDateStr = null;
        String appXml = null;
        try {
            startDateStr = DateUtils.formatDateUTC(start);
            endDateStr = DateUtils.formatDateUTC(end);

            Reader reader = IOUtils.getResourceAsReader("coord-matLookup-trigger.xml", -1);
            appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#start", startDateStr);
            appXml = appXml.replaceAll("#end", endDateStr);
            return appXml;
        }
        catch (Exception ex) {
            fail("Could not get coord-matLookup-trigger.xml" + ex.getMessage());
            throw ex;
        }
    }

    private void addRecordToJobTable(String jobId, CoordinatorStore store, Date start, Date end) throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = getCoordJobXml(appPath, start, end);
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath + "/coordinator.xml")));
        byte[] bytes = appXml.getBytes("UTF-8");
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        Reader reader2 = new InputStreamReader(bais);
        IOUtils.copyCharStream(reader2, writer);

        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.PREP);
        coordJob.setCreatedTime(new Date());
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        coordJob.setAuthToken("notoken");
        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        coordJob.setExecution(Execution.FIFO);
        coordJob.setConcurrency(1);
        coordJob.setStartTime(start);
        coordJob.setEndTime(end);
        try {
            store.insertCoordinatorJob(coordJob);
        }
        catch (StoreException se) {
            se.printStackTrace();
            store.rollbackTrx();
            fail("Unable to insert the test job record to table");
            throw se;
        }
    }

    /**
     * Test current mode. The job should be picked up for materialization.
     *
     * @throws Exception
     */
    public void testCoordJobMatLookupTriggerService2() throws Exception {
        Date start = new Date();
        Date end = new Date(start.getTime() + 3600 * 1000);
        final String jobId = "0000000-" + start.getTime() + "-testCoordRecoveryService-C";
        CoordinatorStore store = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store.beginTrx();
        addRecordToJobTable(jobId, store, start, end);
        store.commitTrx();
        store.closeTrx();

        Thread.sleep(3000);
        Runnable runnable = new CoordJobMatLookupTriggerRunnable(3600);
        runnable.run();
        Thread.sleep(6000);

        CoordinatorStore store2 = Services.get().get(StoreService.class).getStore(CoordinatorStore.class);
        store2.beginTrx();
        CoordinatorJobBean coordJob = store2.getCoordinatorJob(jobId, false);
        store2.commitTrx();
        store2.closeTrx();
        if (!(coordJob.getStatus() == CoordinatorJob.Status.PREMATER)) {
            fail();
        }
    }
}