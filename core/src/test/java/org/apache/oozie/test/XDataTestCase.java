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

package org.apache.oozie.test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.AppType;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Execution;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.SLAEvent;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.BundleActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLAEventInsertJPAExecutor;
import org.apache.oozie.executor.jpa.SLARegistrationQueryExecutor;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.StartNodeDef;
import org.jdom.Element;
import org.jdom.JDOMException;

import com.google.common.annotations.VisibleForTesting;

@SuppressWarnings("deprecation")
public abstract class XDataTestCase extends XHCatTestCase {

    protected static String slaXml = " <sla:info xmlns:sla='uri:oozie:sla:0.1'>"
            + " <sla:app-name>test-app</sla:app-name>" + " <sla:nominal-time>2009-03-06T10:00Z</sla:nominal-time>"
            + " <sla:should-start>5</sla:should-start>" + " <sla:should-end>120</sla:should-end>"
            + " <sla:notification-msg>Notifying User for nominal time : 2009-03-06T10:00Z </sla:notification-msg>"
            + " <sla:alert-contact>abc@example.com</sla:alert-contact>"
            + " <sla:dev-contact>abc@example.com</sla:dev-contact>"
            + " <sla:qa-contact>abc@example.com</sla:qa-contact>" + " <sla:se-contact>abc@example.com</sla:se-contact>"
            + "</sla:info>";

    protected String bundleName;
    protected String bundleId;
    protected String CREATE_TIME = "2012-07-22T00:00Z";

    public XDataTestCase() {
    }

    /*
     * The following 2 methods are "published" versions of
     * #setUp() and #tearDown() respectively. Created to be able to
     * use them in composition.
     */
    @VisibleForTesting
    public void setUpPub() throws Exception {
        setUp();
    }

    @VisibleForTesting
    public void tearDownPub() throws Exception {
        tearDown();
    }

    /**
     * Inserts the passed coord job
     * @param coord job bean
     * @throws Exception
     */
    protected void addRecordToCoordJobTable(CoordinatorJobBean coordJob) throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }
    }

    /**
     * Insert coord job for testing.
     *
     * @param status coord job status
     * @param pending true if pending is true
     * @param doneMatd true if doneMaterialization is true
     * @return coord job bean
     * @throws Exception
     */
    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, boolean pending,
            boolean doneMatd) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, pending, doneMatd);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;
    }

    protected CoordinatorJobBean addRecordToCoordJobTable(String appXml) throws Exception {
        Date start = DateUtils.parseDateOozieTZ("2009-02-01T01:00Z");
        Date end = DateUtils.parseDateOozieTZ("2009-02-03T23:59Z");
        appXml = appXml.replaceAll("#start", DateUtils.formatDateOozieTZ(start));
        appXml = appXml.replaceAll("#end", DateUtils.formatDateOozieTZ(end));
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        writeToFile(appXml, appPath, "coordinator.xml");
        CoordinatorJobBean coordJob = createCoordBean(appPath, appXml, CoordinatorJob.Status.PREP, start, end, false,
                false, 0);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;
    }

    /**
     * Insert coord job for testing.
     *
     * @param status coord job status
     * @param start start time
     * @param end end time
     * @param created Time
     * @param pending true if pending is true
     * @param doneMatd true if doneMaterialization is true
     * @param lastActionNum last action number
     * @return coord job bean
     * @throws Exception
     */
    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date start, Date end,
            boolean pending, boolean doneMatd, int lastActionNum) throws Exception {
        return addRecordToCoordJobTable(status, start, end, new Date(), pending, doneMatd, lastActionNum);
    }

    /**
     * Insert coord job for testing.
     *
     * @param status coord job status
     * @param start start time
     * @param end end time
     * @param created Time
     * @param pending true if pending is true
     * @param doneMatd true if doneMaterialization is true
     * @param lastActionNum last action number
     * @return coord job bean
     * @throws Exception
     */
    protected CoordinatorJobBean addRecordToCoordJobTable(CoordinatorJob.Status status, Date start, Date end,
            Date createdTime, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, start, end, createdTime, pending, doneMatd, lastActionNum);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;

    }

    /**
     * Insert coord job for testing.
     *
     * @param testFileName test file name
     * @param status coord job status
     * @param start start time
     * @param end end time
     * @param pending true if pending is true
     * @param doneMatd true if doneMaterialization is true
     * @param lastActionNum last action number
     * @return coord job bean
     * @throws Exception
     */
    protected CoordinatorJobBean addRecordToCoordJobTable(String testFileName, CoordinatorJob.Status status,
            Date start, Date end, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(testFileName, status, start, end, pending, doneMatd, lastActionNum);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;

    }

    /**
     * Add coordinator job bean with bundle id info.
     *
     * @param bundleId bundle id
     * @param coordId coord id and coord name
     * @param status job status
     * @param pending true if pending is true
     * @param doneMatd true if doneMaterialization is true
     * @param lastActionNumber last action number
     * @return coordinator job bean
     * @throws Exception
     */
    protected CoordinatorJobBean addRecordToCoordJobTableWithBundle(String bundleId, String coordId,
            CoordinatorJob.Status status, boolean pending, boolean doneMatd, int lastActionNumber) throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, pending, doneMatd);
        coordJob.setBundleId(bundleId);
        // coord id and coord name are the same
        coordJob.setId(coordId);
        coordJob.setAppName(coordId);
        coordJob.setLastActionNumber(lastActionNumber);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;
    }

    /**
     * Add coordinator job bean with bundle id info.
     *
     * @param bundleId bundle id
     * @param coordId coord id and coord name
     * @param status job status
     * @param start start time
     * @param end end time
     * @param pending true if pending is true
     * @param doneMatd true if doneMaterialization is true
     * @param lastActionNumber last action number
     * @return coordinator job bean
     * @throws Exception
     */
    protected CoordinatorJobBean addRecordToCoordJobTableWithBundle(String bundleId, String coordId,
            CoordinatorJob.Status status, Date start, Date end, boolean pending, boolean doneMatd, int lastActionNumber)
            throws Exception {
        CoordinatorJobBean coordJob = createCoordJob(status, start, end, pending, doneMatd, 0);
        coordJob.setBundleId(bundleId);
        // coord id and coord name are the same
        coordJob.setId(coordId);
        coordJob.setAppName(coordId);
        coordJob.setLastActionNumber(lastActionNumber);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;
    }

    /**
     * Create coord job bean
     *
     * @param status coord job status
     * @param pending true if pending is true
     * @param doneMatd true if doneMaterialization is true
     * @return coord job bean
     * @throws IOException
     */
    protected CoordinatorJobBean createCoordJob(CoordinatorJob.Status status, boolean pending, boolean doneMatd)
            throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = writeCoordXml(appPath);

        // Set the start and end time in future
        String currentDatePlusMonth = XDataTestCase.getCurrentDateafterIncrementingInMonths(1);
        Date start = DateUtils.parseDateOozieTZ(currentDatePlusMonth);
        Date end = DateUtils.parseDateOozieTZ(currentDatePlusMonth);

        return createCoordBean(appPath, appXml, status, start, end, pending, doneMatd, 0);
    }

    /**
     * Create coord job bean
     *
     * @param status coord job status
     * @param start start time
     * @param end end time
     * @param pending true if pending is true
     * @param doneMatd true if doneMaterialization is true
     * @param lastActionNum last action number
     * @return coord job bean
     * @throws IOException
     */
    protected CoordinatorJobBean createCoordJob(CoordinatorJob.Status status, Date start, Date end, boolean pending,
            boolean doneMatd, int lastActionNum) throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = writeCoordXml(appPath, start, end);

        return createCoordBean(appPath, appXml, status, start, end, pending, doneMatd, lastActionNum);
    }
    /**
     * Create coord job bean
     *
     * @param status coord job status
     * @param start start time
     * @param end end time
     * @param created Time
     * @param pending true if pending is true
     * @param doneMatd true if doneMaterialization is true
     * @param lastActionNum last action number
     * @return coord job bean
     * @throws IOException
     */
    protected CoordinatorJobBean createCoordJob(CoordinatorJob.Status status, Date start, Date end, Date createTime, boolean pending,
            boolean doneMatd, int lastActionNum) throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = writeCoordXml(appPath, start, end);

        return createCoordBean(appPath, appXml, status, start, end, createTime, pending, doneMatd, lastActionNum);
    }

    /**
     * Create coord job bean
     *
     * @param testFileName test file name
     * @param status coord job status
     * @param start start time
     * @param end end time
     * @param pending true if pending is true
     * @param doneMatd true if doneMaterialization is true
     * @param lastActionNum last action number
     * @return coord job bean
     * @throws IOException
     */
    protected CoordinatorJobBean createCoordJob(String testFileName, CoordinatorJob.Status status, Date start,
            Date end, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String appXml = writeCoordXml(appPath, testFileName);

        return createCoordBean(appPath, appXml, status, start, end, pending, doneMatd, lastActionNum);
    }

    private CoordinatorJobBean createCoordBean(Path appPath, String appXml, CoordinatorJob.Status status, Date start,
            Date end, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {
        return createCoordBean(appPath, appXml, status, start, end, new Date(), pending, doneMatd, lastActionNum);
    }

    private CoordinatorJobBean createCoordBean(Path appPath, String appXml, CoordinatorJob.Status status, Date start,
            Date end, Date createdTime, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(Services.get().get(UUIDService.class).generateId(ApplicationType.COORDINATOR));
        coordJob.setAppName("COORD-TEST");
        coordJob.setAppPath(appPath.toString());
        coordJob.setStatus(status);
        coordJob.setTimeZone("America/Los_Angeles");
        coordJob.setCreatedTime(createdTime);
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser(getTestUser());
        coordJob.setGroup(getTestGroup());
        if (pending) {
            coordJob.setPending();
        }
        if (doneMatd) {
            coordJob.setDoneMaterialization();
        }
        coordJob.setLastActionNumber(lastActionNum);

        Configuration conf = getCoordConf(appPath);
        coordJob.setConf(XmlUtils.prettyPrint(conf).toString());
        coordJob.setJobXml(appXml);
        coordJob.setFrequency("1");
        coordJob.setTimeUnit(Timeunit.DAY);
        coordJob.setExecutionOrder(Execution.FIFO);
        coordJob.setConcurrency(1);
        coordJob.setMatThrottling(1);
        try {
            coordJob.setStartTime(start);
            coordJob.setEndTime(end);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Could not set Date/time");
        }
        return coordJob;
    }

    /**
     * Write coordinator xml
     *
     * @param appPath app path
     * @throws IOException thrown if unable to write xml
     * @throws UnsupportedEncodingException thrown if encoding failed
     */
    protected String writeCoordXml(Path appPath) throws IOException, UnsupportedEncodingException {
        String appXml = getCoordJobXml(appPath);

        FileSystem fs = getFileSystem();

        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath + "/coordinator.xml")));
        byte[] bytes = appXml.getBytes("UTF-8");
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        Reader reader2 = new InputStreamReader(bais);
        IOUtils.copyCharStream(reader2, writer);
        return appXml;
    }

    /**
     * Write coordinator xml
     *
     * @param appPath app path
     * @param start start time
     * @param end end time
     * @throws IOException thrown if unable to write xml
     * @throws UnsupportedEncodingException thrown if encoding failed
     */
    protected String writeCoordXml(Path appPath, Date start, Date end) throws IOException, UnsupportedEncodingException {
        String appXml = getCoordJobXml(appPath, start, end);

        FileSystem fs = getFileSystem();

        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath + "/coordinator.xml")));
        byte[] bytes = appXml.getBytes("UTF-8");
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        Reader reader2 = new InputStreamReader(bais);
        IOUtils.copyCharStream(reader2, writer);
        return appXml;
    }

    /**
     * Write coordinator xml
     *
     * @param appPath app path
     * @return testFileName test file name
     * @throws IOException thrown if unable to write xml
     * @throws UnsupportedEncodingException thrown if encoding failed
     */
    protected String writeCoordXml(Path appPath, String testFileName) throws IOException, UnsupportedEncodingException {
        String appXml = getCoordJobXml(testFileName);
        writeToFile(appXml, appPath, "coordinator.xml");
        return appXml;
    }

    /**
     * Insert coord action for testing.
     *
     * @param jobId coord job id
     * @param actionNum action number
     * @param status coord action status
     * @param resourceXmlName xml file name
     * @param pending pending counter
     * @return coord action bean
     * @throws Exception thrown if unable to create coord action bean
     */
    protected CoordinatorActionBean addRecordToCoordActionTable(String jobId, int actionNum,
            CoordinatorAction.Status status, String resourceXmlName, int pending) throws Exception {
        return addRecordToCoordActionTable(jobId, actionNum, status, resourceXmlName, pending, null);
    }

    /**
     * Insert coord action for testing.
     *
     * @param jobId coord job id
     * @param actionNum action number
     * @param status coord action status
     * @param resourceXmlName xml file name
     * @param pending pending counter
     * @param actionNominalTime
     * @return coord action bean
     * @throws Exception thrown if unable to create coord action bean
     */
    protected CoordinatorActionBean addRecordToCoordActionTable(String jobId, int actionNum,
            CoordinatorAction.Status status, String resourceXmlName, int pending, Date actionNominalTime)
            throws Exception {
        CoordinatorActionBean action = createCoordAction(jobId, actionNum, status, resourceXmlName, pending,
                actionNominalTime);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionInsertJPAExecutor coordActionInsertCmd = new CoordActionInsertJPAExecutor(action);
            jpaService.execute(coordActionInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord action record to table");
            throw je;
        }
        return action;
    }

    /**
     * Insert coord action and workflow id as external id for testing.
     *
     * @param jobId coord job id
     * @param actionNum action number
     * @param status coord action status
     * @param resourceXmlName xml file name
     * @param wfId wf id
     * @param wfStatus wf status
     * @param pending pending counter
     * @return coord action bean
     * @throws Exception thrown if unable to create coord action bean
     */
    protected CoordinatorActionBean addRecordToCoordActionTable(String jobId, int actionNum,
            CoordinatorAction.Status status, String resourceXmlName, String wfId, String wfStatus, int pending)
            throws Exception {
        CoordinatorActionBean action = createCoordAction(jobId, actionNum, status, resourceXmlName, pending);
        action.setExternalId(wfId);
        action.setExternalStatus(wfStatus);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionInsertJPAExecutor coordActionInsertExecutor = new CoordActionInsertJPAExecutor(action);
            jpaService.execute(coordActionInsertExecutor);

            if (wfId != null) {
                WorkflowJobBean wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(wfId));
                wfJob.setParentId(action.getId());
                WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_PARENT_MODIFIED, wfJob);
            }
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord action record to table");
            throw je;
        }
        return action;
    }

    protected CoordinatorActionBean createCoordAction(String jobId, int actionNum, CoordinatorAction.Status status,
            String resourceXmlName, int pending) throws Exception {
        return createCoordAction(jobId, actionNum, status, resourceXmlName, pending, "Z", null);
    }

    protected CoordinatorActionBean createCoordAction(String jobId, int actionNum, CoordinatorAction.Status status,
            String resourceXmlName, int pending, Date actionNominalTime) throws Exception {
        return createCoordAction(jobId, actionNum, status, resourceXmlName, pending, "Z", actionNominalTime);
    }


    /**
     * Create coord action bean
     *
     * @param jobId coord job id
     * @param actionNum action number
     * @param status coord action status
     * @param resourceXmlName xml file name
     * @param pending pending counter
     * @return coord action bean
     * @throws Exception thrown if unable to create coord action bean
     */
    protected CoordinatorActionBean createCoordAction(String jobId, int actionNum, CoordinatorAction.Status status,
            String resourceXmlName, int pending, String oozieTimeZoneMask, Date actionNominalTime) throws Exception {
        String actionId = Services.get().get(UUIDService.class).generateChildId(jobId, actionNum + "");
        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, resourceXmlName);
        actionXml = actionXml.replace("${TZ}", oozieTimeZoneMask);

        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setId(actionId);
        if(status != CoordinatorAction.Status.SUBMITTED && status != CoordinatorAction.Status.READY) {
            action.setExternalId(actionId + "_E");
        }
        action.setJobId(jobId);
        action.setActionNumber(actionNum);
        action.setPending(pending);
        try {
            if (actionNominalTime == null) {
                String nominalTime = getActionNominalTime(actionXml);

                action.setNominalTime(DateUtils.parseDateOozieTZ(nominalTime));
            }
            else {
                action.setNominalTime(actionNominalTime);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Unable to get action nominal time");
            throw new IOException(e);
        }
        action.setLastModifiedTime(new Date());
        action.setCreatedTime(new Date());
        action.setStatus(status);
        action.setActionXml(actionXml);
        action.setTimeOut(10);

        Configuration conf = getCoordConf(appPath);
        action.setCreatedConf(XmlUtils.prettyPrint(conf).toString());
        action.setRunConf(XmlUtils.prettyPrint(conf).toString());
        return action;
    }

    /**
     * Insert subwf job for testing.
     *
     * @param jobStatus workflow job status
     * @param instanceStatus workflow instance status
     * @param parentId the id of the parent workflow
     * @return workflow job bean
     * @throws Exception thrown if unable to create workflow job bean
     */
    protected WorkflowJobBean addRecordToWfJobTable(WorkflowJob.Status jobStatus, WorkflowInstance.Status instanceStatus,
            String parentId) throws Exception {
        WorkflowJobBean subwfBean = addRecordToWfJobTable(jobStatus, instanceStatus);
        subwfBean.setParentId(parentId);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowJobQueryExecutor.getInstance().executeUpdate(WorkflowJobQuery.UPDATE_WORKFLOW_PARENT_MODIFIED, subwfBean);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test wf job record to table");
            throw je;
        }
        return subwfBean;
    }

    /**
     * Insert wf job for testing.
     *
     * @param jobStatus workflow job status
     * @param instanceStatus workflow instance status
     * @return workflow job bean
     * @throws Exception thrown if unable to create workflow job bean
     */
    protected WorkflowJobBean addRecordToWfJobTable(WorkflowJob.Status jobStatus, WorkflowInstance.Status instanceStatus)
            throws Exception {
        WorkflowApp app = new LiteWorkflowApp("testApp", "<workflow-app/>", new StartNodeDef(
                LiteWorkflowStoreService.LiteControlNodeHandler.class, "end")).addNode(new EndNodeDef("end",
                LiteWorkflowStoreService.LiteControlNodeHandler.class));
        return addRecordToWfJobTable(app, jobStatus, instanceStatus);
    }

    protected WorkflowJobBean addRecordToWfJobTable(WorkflowApp app, WorkflowJob.Status jobStatus,
            WorkflowInstance.Status instanceStatus) throws Exception {
        Configuration conf = new Configuration();
        Path appUri = new Path(getAppPath(), "workflow.xml");
        conf.set(OozieClient.APP_PATH, appUri.toString());
        conf.set(OozieClient.LOG_TOKEN, "testToken");
        conf.set(OozieClient.USER_NAME, getTestUser());

        WorkflowJobBean wfBean = createWorkflow(app, conf, jobStatus, instanceStatus);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowJobInsertJPAExecutor wfInsertCmd = new WorkflowJobInsertJPAExecutor(wfBean);
            jpaService.execute(wfInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test wf job record to table");
            throw je;
        }
        return wfBean;
    }

    protected Path getAppPath() {
        Path baseDir = getFsTestCaseDir();
        return new Path(baseDir, "app");
    }

    /**
     * Insert wf action for testing.
     *
     * @param wfId workflow id
     * @param actionName workflow action name
     * @param status workflow action status
     * @return workflow action bean
     * @throws Exception thrown if unable to create workflow action bean
     */
    protected WorkflowActionBean addRecordToWfActionTable(String wfId, String actionName, WorkflowAction.Status status)
            throws Exception {
        return addRecordToWfActionTable(wfId, actionName, status, "", false);
    }

    protected WorkflowActionBean addRecordToWfActionTable(String wfId, String actionName, WorkflowAction.Status status,
            boolean pending) throws Exception {
        return addRecordToWfActionTable(wfId, actionName, status, "", pending);
    }

    protected WorkflowActionBean addRecordToWfActionTable(String wfId, String actionName, WorkflowAction.Status status,
            String execPath, boolean pending) throws Exception {
        WorkflowActionBean action = createWorkflowAction(wfId, actionName, status, pending);
        action.setExecutionPath(execPath);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            WorkflowActionInsertJPAExecutor actionInsertCmd = new WorkflowActionInsertJPAExecutor(action);
            jpaService.execute(actionInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test wf action record to table");
            throw je;
        }
        return action;
    }

    /**
     * Insert sla event for testing.
     *
     * @param slaId sla id
     * @param status sla status
     * @throws Exception thrown if unable to create sla bean
     */
    @Deprecated
    protected void addRecordToSLAEventTable(String slaId, SLAEvent.Status status, Date today) throws Exception {
        addRecordToSLAEventTable(slaId, "app-name", status, today);
    }

    /**
     * Insert sla event for testing.
     *
     * @param slaId sla id
     * @param slaId app name
     * @param status sla status
     * @throws Exception thrown if unable to create sla bean
     */
    @Deprecated
    protected void addRecordToSLAEventTable(String slaId, String appName, SLAEvent.Status status, Date today)
            throws Exception {
        SLAEventBean sla = new SLAEventBean();
        sla.setSlaId(slaId);
        sla.setAppName(appName);
        sla.setParentClientId("parent-client-id");
        sla.setParentSlaId("parent-sla-id");
        sla.setExpectedStart(today);
        sla.setExpectedEnd(today);
        sla.setNotificationMsg("notification-msg");
        sla.setAlertContact("alert-contact");
        sla.setDevContact("dev-contact");
        sla.setQaContact("qa-contact");
        sla.setSeContact("se-contact");
        sla.setAlertFrequency("alert-frequency");
        sla.setAlertPercentage("alert-percentage");
        sla.setUpstreamApps("upstream-apps");
        sla.setAppType(SLAEvent.SlaAppType.WORKFLOW_JOB);
        sla.setUser(getTestUser());
        sla.setGroupName(getTestGroup());
        sla.setJobStatus(status);
        sla.setStatusTimestamp(today);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            SLAEventInsertJPAExecutor slaInsertCmd = new SLAEventInsertJPAExecutor(sla);
            jpaService.execute(slaInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test sla event record to table");
            throw je;
        }
    }

    /**
     * Insert sla summary for test
     * @param appName application name
     * @param status  sla status
     * @return
     * @throws Exception
     */
    protected SLASummaryBean addRecordToSLASummaryTable(String appName, SLAStatus status)
            throws Exception {
        SLASummaryBean sla = new SLASummaryBean();
        Date today = new Date();
        sla.setId(Services.get().get(UUIDService.class).generateId(ApplicationType.COORDINATOR));
        sla.setAppName(appName);
        sla.setAppType(AppType.COORDINATOR_JOB);
        sla.setCreatedTime(today);
        sla.setNominalTime(today);
        sla.setExpectedStart(today);
        sla.setExpectedEnd(today);
        sla.setExpectedDuration(100);
        sla.setJobStatus("RUNNING");
        sla.setSLAStatus(status);
        sla.setEventStatus(EventStatus.START_MET);
        sla.setLastModifiedTime(today);
        sla.setUser("oozie");
        sla.setParentId(Services.get().get(UUIDService.class).generateId(ApplicationType.BUNDLE));
        sla.setEventProcessed(1);
        sla.setActualStart(today);
        sla.setActualEnd(today);
        sla.setActualDuration(100);
        try {
            SLASummaryQueryExecutor.getInstance().insert(sla);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test sla event record to table");
            throw je;
        }
        return sla;
    }

    /**
     * Insert sla registration for test
     * @param appName application name
     * @param status  sla status
     * @return
     * @throws Exception
     */
    protected SLARegistrationBean addRecordToSLARegistrationTable(String appName, SLAStatus status)
            throws Exception {
        SLARegistrationBean sla = new SLARegistrationBean();
        Date today = new Date();
        sla.setId(Services.get().get(UUIDService.class).generateId(ApplicationType.COORDINATOR));
        sla.setAppName(appName);
        sla.setAppType(AppType.COORDINATOR_JOB);
        sla.setExpectedDuration(100);
        sla.setExpectedEnd(today);
        sla.setExpectedStart(today);
        sla.setJobData("test-job-data");
        sla.setCreatedTime(today);
        sla.setNominalTime(today);
        sla.setNotificationMsg("test-sla-notification-msg");
        sla.setParentId(Services.get().get(UUIDService.class).generateId(ApplicationType.BUNDLE));
        sla.setSlaConfig("alert-events");
        sla.setUpstreamApps("test-upstream-apps");
        sla.setUser("oozie");
        try {
            SLARegistrationQueryExecutor.getInstance().insert(sla);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test sla registration record to table");
            throw je;
        }
        return sla;
    }

    /**
     * Insert bundle job for testing.
     *
     * @param jobStatus job status
     * @param pending true if pending
     * @return bundle job bean
     * @throws Exception
     */
    public BundleJobBean addRecordToBundleJobTable(Job.Status jobStatus, boolean pending) throws Exception {
        BundleJobBean bundle = createBundleJob(jobStatus, pending);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            BundleJobInsertJPAExecutor bundleInsertjpa = new BundleJobInsertJPAExecutor(bundle);
            jpaService.execute(bundleInsertjpa);
        }
        catch (JPAExecutorException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test bundle job record to table");
            throw ce;
        }
        return bundle;
    }

    /**
     * Insert a bad bundle job for testing negative cases.
     *
     * @param jobStatus job status
     * @return bundle job bean
     * @throws Exception
     */
    protected BundleJobBean addRecordToBundleJobTableNegative(Job.Status jobStatus) throws Exception {
        BundleJobBean bundle = createBundleJobNegative(jobStatus);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            BundleJobInsertJPAExecutor bundleInsertjpa = new BundleJobInsertJPAExecutor(bundle);
            jpaService.execute(bundleInsertjpa);
        }
        catch (JPAExecutorException ce) {
            ce.printStackTrace();
            fail("Unable to insert the test bundle job record to table");
            throw ce;
        }
        return bundle;
    }

    /**
     * Create bundle action bean and save to db
     *
     * @param jobId bundle job id
     * @param coordName coordinator name
     * @param pending true if action is pending
     * @param status job status
     * @return bundle action bean
     * @throws Exception
     */
    protected BundleActionBean addRecordToBundleActionTable(String jobId, String coordName, int pending,
            Job.Status status) throws Exception {
        BundleActionBean action = createBundleAction(jobId, null, coordName, pending, status);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            BundleActionInsertJPAExecutor bundleActionJPAExecutor = new BundleActionInsertJPAExecutor(action);
            jpaService.execute(bundleActionJPAExecutor);
        }
        catch (JPAExecutorException ex) {
            ex.printStackTrace();
            fail("Unable to insert the test bundle action record to table");
            throw ex;
        }

        return action;
    }

    /**
     * Create bundle action bean and save to db
     *
     * @param jobId bundle job id
     * @param coordId coordinator id
     * @param coordName coordinator name
     * @param pending true if action is pending
     * @param status job status
     * @return bundle action bean
     * @throws Exception
     */
    protected BundleActionBean addRecordToBundleActionTable(String jobId, String coordId, String coordName, int pending,
            Job.Status status) throws Exception {
        BundleActionBean action = createBundleAction(jobId, coordId, coordName, pending, status);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            BundleActionInsertJPAExecutor bundleActionJPAExecutor = new BundleActionInsertJPAExecutor(action);
            jpaService.execute(bundleActionJPAExecutor);

            CoordinatorJobBean coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordId));
            coordJob.setBundleId(jobId);
            CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB_BUNDLEID, coordJob);
        }
        catch (JPAExecutorException ex) {
            ex.printStackTrace();
            fail("Unable to insert the test bundle action record to table");
            throw ex;
        }

        return action;
    }

    /**
     * Create bundle action bean
     *
     * @param jobId bundle job id
     * @param coordId coordinator id
     * @param coordName coordinator name
     * @param pending true if action is pending
     * @param status job status
     * @return bundle action bean
     * @throws Exception
     */
    protected BundleActionBean createBundleAction(String jobId, String coordId, String coordName, int pending, Job.Status status)
            throws Exception {
        BundleActionBean action = new BundleActionBean();
        action.setBundleId(jobId);
        action.setBundleActionId(jobId + "_" + coordName);
        action.setPending(pending);
        action.setCoordId(coordId);
        action.setCoordName(coordName);
        action.setStatus(status);
        action.setLastModifiedTime(new Date());

        return action;
    }

    /**
     * Read coord job xml from test resources
     *
     * @param appPath application path
     * @return content of coord job xml
     */
    protected String getCoordJobXml(Path appPath) {
        String inputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        inputTemplate = Matcher.quoteReplacement(inputTemplate);
        String outputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        outputTemplate = Matcher.quoteReplacement(outputTemplate);
        try {
            Reader reader = IOUtils.getResourceAsReader("coord-rerun-job.xml", -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#inputTemplate", inputTemplate);
            appXml = appXml.replaceAll("#outputTemplate", outputTemplate);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get coord-rerun-job.xml", ioe));
        }
    }

    /**
     * Read coord job xml from test resources
     *
     * @param appPath application path
     * @param start start time
     * @param end end time
     * @return content of coord job xml
     */
    protected String getCoordJobXml(Path appPath, Date start, Date end) {
        String startDateStr = null, endDateStr = null;
        try {
            startDateStr = DateUtils.formatDateOozieTZ(start);
            endDateStr = DateUtils.formatDateOozieTZ(end);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Could not format dates");
        }
        try {
            Reader reader = IOUtils.getResourceAsReader("coord-matd-job.xml", -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#start", startDateStr);
            appXml = appXml.replaceAll("#end", endDateStr);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get coord-matd-job.xml", ioe));
        }
    }

    /**
     * Read coord job xml from test resources
     *
     * @param testFileName file name of coord job xml
     * @return content of coord job xml
     */
    protected String getCoordJobXml(String testFileName) {
        try {
            Reader reader = IOUtils.getResourceAsReader(testFileName, -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get [{0}]", testFileName, ioe));
        }
    }

    /**
     * Read coord action xml from test resources
     *
     * @param appPath application path
     * @param resourceXmlName file name of coord action xml
     * @return content of coord action xml
     */
    protected String getCoordActionXml(Path appPath, String resourceXmlName) {
        String inputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        inputTemplate = Matcher.quoteReplacement(inputTemplate);
        String outputTemplate = appPath + "/coord-input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        outputTemplate = Matcher.quoteReplacement(outputTemplate);

        String inputDir = appPath + "/coord-input/2010/07/05/01/00";
        inputDir = Matcher.quoteReplacement(inputDir);
        String outputDir = appPath + "/coord-input/2009/12/14/11/00";
        outputDir = Matcher.quoteReplacement(outputDir);
        try {
            Reader reader = IOUtils.getResourceAsReader(resourceXmlName, -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#inputTemplate", inputTemplate);
            appXml = appXml.replaceAll("#outputTemplate", outputTemplate);
            appXml = appXml.replaceAll("#inputDir", inputDir);
            appXml = appXml.replaceAll("#outputDir", outputDir);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get " + resourceXmlName, ioe));
        }
    }

    /**
     * Get coordinator configuration
     *
     * @param appPath application path
     * @return coordinator configuration
     * @throws IOException thrown if unable to get coord conf
     */
    protected Configuration getCoordConf(Path appPath) throws IOException {
        Path wfAppPath = new Path(getFsTestCaseDir(), "coord");

        Configuration jobConf = new XConfiguration();
        jobConf.set(OozieClient.COORDINATOR_APP_PATH, appPath.toString());
        jobConf.set(OozieClient.USER_NAME, getTestUser());
        jobConf.set("jobTracker", getJobTrackerUri());
        jobConf.set("nameNode", getNameNodeUri());
        jobConf.set("wfAppPath", wfAppPath.toString());

        String content = "<workflow-app xmlns='uri:oozie:workflow:0.1'  xmlns:sla='uri:oozie:sla:0.1' name='no-op-wf'>";
        content += "<start to='end' />";
        content += "<end name='end' /></workflow-app>";
        writeToFile(content, wfAppPath, "workflow.xml");

        return jobConf;
    }

    protected void writeToFile(String content, Path appPath, String fileName) throws IOException {
        FileSystem fs = getFileSystem();
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, fileName), true));
        writer.write(content);
        writer.close();
    }

    /**
     * Get action nominal time.
     *
     * @param actionXml
     * @return
     */
    protected String getActionNominalTime(String actionXml) {
        Element eAction;
        try {
            eAction = XmlUtils.parseXml(actionXml);
        }
        catch (JDOMException je) {
            throw new RuntimeException(XLog.format("Could not parse actionXml :" + actionXml, je));
        }
        String actionNomialTime = eAction.getAttributeValue("action-nominal-time");

        return actionNomialTime;
    }

    /**
     * Create workflow job bean
     *
     * @param app workflow app
     * @param conf workflow configuration
     * @param authToken auth token
     * @param jobStatus workflow job status
     * @param instanceStatus workflow instance status
     * @return workflow job bean
     * @throws Exception thrown if unable to create workflow job bean
     */
    protected WorkflowJobBean createWorkflow(WorkflowApp app, Configuration conf,
            WorkflowJob.Status jobStatus, WorkflowInstance.Status instanceStatus) throws Exception {
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        Configuration protoActionConf = wps.createProtoActionConf(conf, true);
        WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();
        WorkflowInstance wfInstance = workflowLib.createInstance(app, conf);
        ((LiteWorkflowInstance) wfInstance).setStatus(instanceStatus);
        WorkflowJobBean workflow = new WorkflowJobBean();
        workflow.setId(wfInstance.getId());
        workflow.setExternalId("extid");
        workflow.setAppName(app.getName());
        workflow.setAppPath(conf.get(OozieClient.APP_PATH));
        workflow.setConf(XmlUtils.prettyPrint(conf).toString());
        workflow.setProtoActionConf(XmlUtils.prettyPrint(protoActionConf).toString());
        workflow.setCreatedTime(new Date());
        workflow.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
        workflow.setStatus(jobStatus);
        workflow.setRun(0);
        workflow.setUser(conf.get(OozieClient.USER_NAME));
        workflow.setGroup(conf.get(OozieClient.GROUP_NAME));
        workflow.setWorkflowInstance(wfInstance);
        workflow.setSlaXml("<sla></sla>");
        return workflow;
    }

    /**
     * Create workflow action bean
     *
     * @param wfId workflow job id
     * @param actionName workflow action name
     * @param status workflow action status
     * @return workflow action bean
     * @throws Exception thrown if unable to create workflow action bean
     */
    protected WorkflowActionBean createWorkflowAction(String wfId, String actionName, WorkflowAction.Status status,
            boolean pending) throws Exception {
        WorkflowActionBean action = new WorkflowActionBean();
        action.setName(actionName);
        action.setId(Services.get().get(UUIDService.class).generateChildId(wfId, actionName));
        action.setJobId(wfId);
        action.setType("map-reduce");
        action.setTransition("transition");
        action.setStatus(status);
        final Date currDate = new Date();
        action.setCreatedTime(currDate);
        action.setStartTime(currDate);
        action.setEndTime(currDate);
        action.setLastCheckTime(currDate);
        action.setCred("null");
        action.setStats("dummyStats");
        if (pending) {
            action.setPending();
        }
        else {
            action.resetPending();
        }

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");

        FileSystem fs = getFileSystem();
        Writer w = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
        w.write("dummy\n");
        w.write("dummy\n");
        w.close();

        String actionXml = "<map-reduce>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "<configuration>"
                + "<property><name>mapred.mapper.class</name><value>" + MapperReducerForTest.class.getName()
                + "</value></property>" + "<property><name>mapred.reducer.class</name><value>"
                + MapperReducerForTest.class.getName() + "</value></property>"
                + "<property><name>mapred.input.dir</name><value>" + inputDir.toString() + "</value></property>"
                + "<property><name>mapred.output.dir</name><value>" + outputDir.toString() + "</value></property>"
                + "</configuration>" + "</map-reduce>";
        action.setConf(actionXml);
        action.setSlaXml("<sla></sla>");
        action.setData("dummy data");
        action.setStats("dummy stats");
        action.setExternalChildIDs("00000001-dummy-oozie-wrkf-W");
        action.setRetries(2);
        action.setUserRetryCount(1);
        action.setUserRetryMax(2);
        action.setUserRetryInterval(1);
        action.setErrorInfo("dummyErrorCode", "dummyErrorMessage");
        action.setExternalId("dummy external id");
        action.setExternalStatus("RUNNING");

        return action;
    }

    protected WorkflowActionBean createWorkflowAction(String wfId, String actionName, WorkflowAction.Status status)
            throws Exception {
        return createWorkflowAction(wfId, actionName, status, false);
    }

    /**
     * Create bundle job bean
     *
     * @param jobStatus job status
     * @param pending true if pending
     * @return bundle job bean
     * @throws Exception
     */
    protected BundleJobBean createBundleJob(String jobID, Job.Status jobStatus, boolean pending) throws Exception {
        Path coordPath1 = new Path(getFsTestCaseDir(), "coord1");
        Path coordPath2 = new Path(getFsTestCaseDir(), "coord2");
        writeCoordXml(coordPath1, "coord-job-bundle.xml");
        writeCoordXml(coordPath2, "coord-job-bundle.xml");

        Path bundleAppPath = new Path(getFsTestCaseDir(), "bundle");
        String bundleAppXml = getBundleXml("bundle-submit-job.xml");
        assertNotNull(bundleAppXml);
        assertTrue(bundleAppXml.length() > 0);

        bundleAppXml = bundleAppXml
                .replaceAll("#app_path1", Matcher.quoteReplacement(new Path(coordPath1.toString(), "coordinator.xml").toString()));
        bundleAppXml = bundleAppXml
                .replaceAll("#app_path2", Matcher.quoteReplacement(new Path(coordPath2.toString(), "coordinator.xml").toString()));

        writeToFile(bundleAppXml, bundleAppPath, "bundle.xml");

        Configuration conf = new XConfiguration();
        conf.set(OozieClient.BUNDLE_APP_PATH, bundleAppPath.toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("jobTracker", getJobTrackerUri());
        conf.set("nameNode", getNameNodeUri());
        conf.set("appName", "bundle-app-name");
        conf.set("coordName1", "coord1");
        conf.set("coordName2", "coord2");

        BundleJobBean bundle = new BundleJobBean();
        bundle.setId(jobID);
        bundle.setAppName("BUNDLE-TEST");
        bundle.setAppPath(bundleAppPath.toString());
        bundle.setConf(XmlUtils.prettyPrint(conf).toString());
        bundle.setConsoleUrl("consoleUrl");
        bundle.setCreatedTime(new Date());
        // TODO bundle.setStartTime(startTime);
        // TODO bundle.setEndTime(endTime);
        // TODO bundle.setExternalId(externalId);
        bundle.setJobXml(bundleAppXml);
        bundle.setLastModifiedTime(new Date());
        bundle.setOrigJobXml(bundleAppXml);
        if (pending) {
            bundle.setPending();
        }
        else {
            bundle.resetPending();
        }
        bundle.setStatus(jobStatus);
        bundle.setUser(conf.get(OozieClient.USER_NAME));
        bundle.setGroup(conf.get(OozieClient.GROUP_NAME));

        return bundle;
    }

    /**
     * Create bundle job bean
     * @param jobStatus
     * @param pending
     * @return
     * @throws Exception
     */
    protected BundleJobBean createBundleJob(Job.Status jobStatus, boolean pending) throws Exception {
        return createBundleJob(Services.get().get(UUIDService.class).generateId(ApplicationType.BUNDLE), jobStatus, pending);
    }


    /**
     * Create bundle job that contains bad coordinator jobs
     *
     * @param jobStatus
     * @return bundle job bean
     * @throws Exception
     */
    protected BundleJobBean createBundleJobNegative(Job.Status jobStatus) throws Exception {
        Path coordPath1 = new Path(getFsTestCaseDir(), "coord1");
        Path coordPath2 = new Path(getFsTestCaseDir(), "coord2");
        writeCoordXml(coordPath1, "coord-job-bundle-negative.xml");
        writeCoordXml(coordPath2, "coord-job-bundle-negative.xml");

        Path bundleAppPath = new Path(getFsTestCaseDir(), "bundle");
        String bundleAppXml = getBundleXml("bundle-submit-job.xml");

        bundleAppXml = bundleAppXml
                .replaceAll("#app_path1", new Path(coordPath1.toString(), "coordinator.xml").toString());
        bundleAppXml = bundleAppXml
                .replaceAll("#app_path2", new Path(coordPath1.toString(), "coordinator.xml").toString());
        writeToFile(bundleAppXml, bundleAppPath, "bundle.xml");

        Configuration conf = new XConfiguration();
        conf.set(OozieClient.BUNDLE_APP_PATH, bundleAppPath.toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("jobTracker", getJobTrackerUri());
        conf.set("nameNode", getNameNodeUri());
        conf.set("coordName1", "coord1");
        conf.set("coordName2", "coord2");

        BundleJobBean bundle = new BundleJobBean();
        bundle.setId(Services.get().get(UUIDService.class).generateId(ApplicationType.BUNDLE));
        bundle.setAppName("BUNDLE-TEST");
        bundle.setAppPath(bundleAppPath.toString());
        bundle.setConf(XmlUtils.prettyPrint(conf).toString());
        bundle.setConsoleUrl("consoleUrl");
        bundle.setCreatedTime(new Date());
        bundle.setJobXml(bundleAppXml);
        bundle.setLastModifiedTime(new Date());
        bundle.setOrigJobXml(bundleAppXml);
        bundle.resetPending();
        bundle.setStatus(jobStatus);
        bundle.setUser(conf.get(OozieClient.USER_NAME));
        bundle.setGroup(conf.get(OozieClient.GROUP_NAME));

        return bundle;
    }

    protected String getBundleXml(String resourceXmlName) {
        try {
            Reader reader = IOUtils.getResourceAsReader(resourceXmlName, -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get " + resourceXmlName, ioe));
        }
    }

    /**
     * Inserts a record to coord action table
     *
     * @param action the record to be inserted
     * @throws Exception
     */
    protected void insertRecordCoordAction(CoordinatorActionBean action) throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionInsertJPAExecutor coordActionInsertCmd = new CoordActionInsertJPAExecutor(action);
            jpaService.execute(coordActionInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord action record to table");
            throw je;
        }
    }

    // Exclude some of the services classes from loading so they dont interfere
    // while the test case is running
    protected void setClassesToBeExcluded(Configuration conf, String[] excludedServices) {
        String classes = conf.get(Services.CONF_SERVICE_CLASSES);
        StringBuilder builder = new StringBuilder(classes);
        for (String s : excludedServices) {
            int index = builder.indexOf(s);
            if (index != -1) {
                builder.replace(index, index + s.length() + 1, "");
            }
        }
        conf.set(Services.CONF_SERVICE_CLASSES, new String(builder));
    }

   /**
    * Add a particular service class to be run in addition to default ones
    * @param conf
    * @param serviceName
    */
   protected void addServiceToRun(Configuration conf, String serviceName) {
       String classes = conf.get(Services.CONF_SERVICE_CLASSES);
       StringBuilder builder = new StringBuilder(classes);
       builder.append("," + serviceName);
       conf.set(Services.CONF_SERVICE_CLASSES, new String(builder));
   }

    /**
     * Adds the db records for the Bulk Monitor tests
     */
    protected void addRecordsForBulkMonitor() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        // adding the bundle job
        BundleJobBean bundle = addRecordToBundleJobTable(BundleJob.Status.RUNNING, false);
        bundleId = bundle.getId();
        bundleName = bundle.getAppName();
        addCoordForBulkMonitor(bundleId);
    }

    protected void addCoordForBulkMonitor(String bundleId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        // adding coordinator job(s) for this bundle
        addRecordToCoordJobTableWithBundle(bundleId, "Coord1", CoordinatorJob.Status.RUNNING, true, true, 2);
        addRecordToCoordJobTableWithBundle(bundleId, "Coord2", CoordinatorJob.Status.RUNNING, true, true, 1);
        addRecordToCoordJobTableWithBundle(bundleId, "Coord3", CoordinatorJob.Status.RUNNING, true, true, 1);

        // adding coordinator action #1 to Coord#1
        CoordinatorActionBean action1 = new CoordinatorActionBean();
        action1.setId("Coord1@1");
        action1.setStatus(CoordinatorAction.Status.FAILED);
        action1.setCreatedTime(DateUtils.parseDateUTC(CREATE_TIME));
        action1.setJobId("Coord1");

        Calendar cal = Calendar.getInstance();
        cal.setTime(DateUtils.parseDateUTC(CREATE_TIME));
        cal.add(Calendar.DATE, -1);

        action1.setNominalTime(cal.getTime());
        CoordActionInsertJPAExecutor actionInsert = new CoordActionInsertJPAExecutor(action1);
        jpaService.execute(actionInsert);

        // adding coordinator action #2 to Coord#1
        CoordinatorActionBean action2 = new CoordinatorActionBean();
        action2.setId("Coord1@2");
        action2.setStatus(CoordinatorAction.Status.KILLED);
        action2.setCreatedTime(DateUtils.parseDateUTC(CREATE_TIME));
        action2.setJobId("Coord1");

        cal.setTime(DateUtils.parseDateUTC(CREATE_TIME));
        cal.add(Calendar.DATE, -1);

        action2.setNominalTime(cal.getTime());
        actionInsert = new CoordActionInsertJPAExecutor(action2);
        jpaService.execute(actionInsert);

        // adding coordinator action #3 to Coord#2
        CoordinatorActionBean action3 = new CoordinatorActionBean();
        action3.setId("Coord2@1");
        action3.setStatus(CoordinatorAction.Status.KILLED);
        action3.setCreatedTime(DateUtils.parseDateUTC(CREATE_TIME));
        action3.setJobId("Coord2");

        cal.setTime(DateUtils.parseDateUTC(CREATE_TIME));
        cal.add(Calendar.DATE, -1);

        action3.setNominalTime(cal.getTime());
        actionInsert = new CoordActionInsertJPAExecutor(action3);
        jpaService.execute(actionInsert);
    }

    /**
     * Add a month to the current time
     *
     * @param incrementMonth
     * @return
     */
    protected static String getCurrentDateafterIncrementingInMonths(int incrementMonth) {
        Calendar currentDate = Calendar.getInstance();
        currentDate.set(Calendar.MONTH, currentDate.get(Calendar.MONTH) + incrementMonth);
        return DateUtils.formatDateOozieTZ(currentDate);
    }

    protected String addInitRecords(String pushMissingDependencies) throws Exception {
        return addInitRecords(null, pushMissingDependencies, "Z");
    }

    protected String addInitRecords(String missingDependencies, String pushMissingDependencies, String oozieTimeZoneMask)
            throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                CoordinatorJob.Status.RUNNING, false, true);

        return addInitRecords(missingDependencies, pushMissingDependencies, oozieTimeZoneMask, job, 1);
    }

    protected String addInitRecords(String missingDependencies, String pushMissingDependencies, String oozieTimeZoneMask,
            CoordinatorJobBean job, int actionNum) throws Exception {
        CoordinatorActionBean action = addRecordToCoordActionTableForWaiting(job.getId(), actionNum,
                CoordinatorAction.Status.WAITING, "coord-action-for-action-input-check.xml", missingDependencies,
                pushMissingDependencies, oozieTimeZoneMask);
        return action.getId();
    }

    protected CoordinatorActionBean addRecordToCoordActionTableForWaiting(String jobId, int actionNum,
            CoordinatorAction.Status status, String resourceXmlName, String missingDependencies,
            String pushMissingDependencies, String oozieTimeZoneMask) throws Exception {
        CoordinatorActionBean action = createCoordAction(jobId, actionNum, status, resourceXmlName, 0,
                oozieTimeZoneMask, null);
        action.setMissingDependencies(missingDependencies);
        action.setPushMissingDependencies(pushMissingDependencies);
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordActionInsertJPAExecutor coordActionInsertCmd = new CoordActionInsertJPAExecutor(action);
            jpaService.execute(coordActionInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord action record to table");
            throw je;
        }
        return action;
    }

    protected CoordinatorJobBean addRecordToCoordJobTableForWaiting(String testFileName, CoordinatorJob.Status status,
             boolean pending, boolean doneMatd) throws Exception {

        String testDir = getTestCaseDir();
        CoordinatorJobBean coordJob = createCoordJob(status, pending, doneMatd);
        String appXml = getCoordJobXmlForWaiting(testFileName, testDir);
        coordJob.setJobXml(appXml);

        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            assertNotNull(jpaService);
            CoordJobInsertJPAExecutor coordInsertCmd = new CoordJobInsertJPAExecutor(coordJob);
            jpaService.execute(coordInsertCmd);
        }
        catch (JPAExecutorException je) {
            je.printStackTrace();
            fail("Unable to insert the test coord job record to table");
            throw je;
        }

        return coordJob;
    }

    protected String getCoordJobXmlForWaiting(String testFileName, String testDir) {
        try {
            Reader reader = IOUtils.getResourceAsReader(testFileName, -1);
            String appXml = IOUtils.getReaderAsString(reader, -1);
            appXml = appXml.replaceAll("#testDir", testDir);
            return appXml;
        }
        catch (IOException ioe) {
            throw new RuntimeException(XLog.format("Could not get " + testFileName, ioe));
        }
    }

    protected void setCoordActionCreationTime(String actionId, long actionCreationTime) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
        action.setCreatedTime(new Date(actionCreationTime));
        CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION, action);
    }

    protected void setCoordActionNominalTime(String actionId, long actionNominalTime) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
        action.setNominalTime(new Date(actionNominalTime));
        CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION, action);
    }

    protected void setMissingDependencies(String actionId, String missingDependencies) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
        action.setMissingDependencies(missingDependencies);
        CoordActionQueryExecutor.getInstance().executeUpdate(CoordActionQuery.UPDATE_COORD_ACTION, action);
    }

    protected void modifyCoordForRunning(CoordinatorJobBean coord) throws Exception {
        String wfXml = IOUtils.getResourceAsString("wf-credentials.xml", -1);
        writeToFile(wfXml, getFsTestCaseDir(), "workflow.xml");
        String coordXml = coord.getJobXml();
        coord.setJobXml(coordXml.replace("hdfs:///tmp/workflows/", getFsTestCaseDir() + "/workflow.xml"));
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coord);
    }

    protected void writeToFile(String appXml, File appPathFile) throws Exception {
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(appPathFile));
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
