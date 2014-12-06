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

package org.apache.oozie.executor.jpa;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorEngine.FILTER_COMPARATORS;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Pair;
import org.junit.Assert;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCoordJobGetActionsSubsetJPAExecutor extends XDataTestCase {
    private static final Pair<String, FILTER_COMPARATORS> POSITIVE_STATUS_FILTER =
        Pair.of(OozieClient.FILTER_STATUS, FILTER_COMPARATORS.EQUALS);
    private static final Pair<String, FILTER_COMPARATORS> NEGATIVE_STATUS_FILTER =
        Pair.of(OozieClient.FILTER_STATUS, FILTER_COMPARATORS.NOT_EQUALS);
    Services services;

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

    private Timestamp getSqlTime(String str) throws ParseException {
        return new Timestamp(DateUtils.parseDateUTC(str).getTime());
    }

    private List<Object> getList(Object... objs) {
        List<Object> list = new ArrayList<Object>();
        for (Object o : objs) {
            list.add(o);
        }
        return list;
    }

    public void testGetActionsWithNominalTimeFilter() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        String timeStr[] = {"2009-02-01T00:00Z", "2009-02-01T05:00Z", "2009-02-01T10:00Z", "2009-02-01T15:00Z"};
        Date ntime[] = {getSqlTime(timeStr[0]), getSqlTime(timeStr[1]), getSqlTime(timeStr[2]), getSqlTime(timeStr[3])};

        List<String> actionIds = new ArrayList<String>(timeStr.length);
        int startAction = 5;
        for (Date time : ntime) {
            CoordinatorActionBean action =
                addRecordToCoordActionTable(job.getId(), startAction++, Status.WAITING, "coord-action-get.xml", 0,
                    time);
            actionIds.add(action.getId());
        }
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        //Filter with nominalTime >=, asc
        Map<Pair<String, FILTER_COMPARATORS>, List<Object>> filterMap = new HashMap<Pair<String, FILTER_COMPARATORS>,
            List<Object>>();
        filterMap.put(
            Pair.of(OozieClient.FILTER_NOMINAL_TIME, FILTER_COMPARATORS.GREATER_EQUAL), getList(ntime[2]));
        CoordJobGetActionsSubsetJPAExecutor actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(),
            filterMap, 1, 10, false);
        List<CoordinatorActionBean> actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(2, actions.size());
        Assert.assertEquals(actionIds.get(2), actions.get(0).getId());
        Assert.assertEquals(actionIds.get(3), actions.get(1).getId());

        //Filter with nominalTime >=, desc
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 1, 10, true);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(2, actions.size());
        Assert.assertEquals(actionIds.get(3), actions.get(0).getId());
        Assert.assertEquals(actionIds.get(2), actions.get(1).getId());

        //Filter with nominalTime <, asc
        filterMap.clear();
        filterMap.put(
            Pair.of(OozieClient.FILTER_NOMINAL_TIME, FILTER_COMPARATORS.LESSTHAN), getList(ntime[2]));
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 1, 10, false);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(2, actions.size());
        Assert.assertEquals(actionIds.get(0), actions.get(0).getId());
        Assert.assertEquals(actionIds.get(1), actions.get(1).getId());

        //Filter with nominalTime <, desc
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 1, 10, true);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(2, actions.size());
        Assert.assertEquals(actionIds.get(1), actions.get(0).getId());
        Assert.assertEquals(actionIds.get(0), actions.get(1).getId());

        //Filter with nominalTime >=, nominalTime <, asc
        filterMap.put(
           Pair.of(OozieClient.FILTER_NOMINAL_TIME, FILTER_COMPARATORS.GREATER_EQUAL), getList(ntime[1]));
        filterMap.put(
            Pair.of(OozieClient.FILTER_NOMINAL_TIME, FILTER_COMPARATORS.LESSTHAN), getList(ntime[3]));
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 1, 10, false);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(2, actions.size());
        Assert.assertEquals(actionIds.get(1), actions.get(0).getId());
        Assert.assertEquals(actionIds.get(2), actions.get(1).getId());

        //Filter with nominalTime >=, nominalTime <, desc
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 1, 10, true);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(2, actions.size());
        Assert.assertEquals(actionIds.get(2), actions.get(0).getId());
        Assert.assertEquals(actionIds.get(1), actions.get(1).getId());

        //Filter with nominalTime >=, nominalTime <, desc, offset
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 2, 10, true);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(1, actions.size());
        Assert.assertEquals(actionIds.get(1), actions.get(0).getId());

        //Filter with nominalTime >=, nominalTime <, asc, offset
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 2, 10, false);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(1, actions.size());
        Assert.assertEquals(actionIds.get(2), actions.get(0).getId());

        //Filter with nominalTime >=, nominalTime <, desc, len
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 1, 2, true);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(2, actions.size());
        Assert.assertEquals(actionIds.get(2), actions.get(0).getId());
        Assert.assertEquals(actionIds.get(1), actions.get(1).getId());

        //Filter with nominalTime >=, nominalTime <, asc, len
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 1, 2, false);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(2, actions.size());
        Assert.assertEquals(actionIds.get(1), actions.get(0).getId());
        Assert.assertEquals(actionIds.get(2), actions.get(1).getId());

        //Filter with nominalTime >=, nominalTime <, asc, offset, len
        filterMap.put(
            Pair.of(OozieClient.FILTER_NOMINAL_TIME, FILTER_COMPARATORS.LESSTHAN),
            getList(getSqlTime("2009-02-01T23:00Z")));
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 2, 2, false);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(2, actions.size());
        Assert.assertEquals(actionIds.get(2), actions.get(0).getId());
        Assert.assertEquals(actionIds.get(3), actions.get(1).getId());

        //Filter with nominalTime >=, nominalTime <, desc, offset, len
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 2, 2, true);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(2, actions.size());
        Assert.assertEquals(actionIds.get(2), actions.get(0).getId());
        Assert.assertEquals(actionIds.get(1), actions.get(1).getId());

        //Filter with nominalTime and status
        filterMap.put(
            Pair.of(OozieClient.FILTER_STATUS, FILTER_COMPARATORS.EQUALS), getList(Status.SUCCEEDED.name()));
        actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(job.getId(), filterMap, 1, 10, true);
        actions = jpaService.execute(actionGetCmd);
        Assert.assertEquals(0, actions.size());

    }

    public void testCoordActionGet() throws Exception {
        int actionNum = 1;
        String resourceXmlName = "coord-action-get.xml";
        Date dummyCreationTime = new Date();
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = createCoordAction(job.getId(), actionNum, CoordinatorAction.Status.WAITING, resourceXmlName, 0);
        // Add some attributes
        action.setConsoleUrl("consoleUrl");
        action.setExternalStatus("externalStatus");
        action.setErrorCode("errorCode");
        action.setErrorMessage("errorMessage");
        action.setTrackerUri("trackerUri");
        action.setCreatedTime(dummyCreationTime);
        String missDeps = getTestCaseFileUri("2009/29/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/22/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/15/_SUCCESS") + "#"
                + getTestCaseFileUri("2009/08/_SUCCESS");
        action.setMissingDependencies(missDeps);
        action.setTimeOut(10);
        // Insert the action
        insertRecordCoordAction(action);

        Path appPath = new Path(getFsTestCaseDir(), "coord");
        String actionXml = getCoordActionXml(appPath, resourceXmlName);
        String actionNominalTime = getActionNominalTime(actionXml);
        //Pass expected values
        _testGetActionsSubset(job.getId(), action.getId(), 1, 1, "consoleUrl", "errorCode", "errorMessage",
                action.getId() + "_E", "externalStatus", "trackerUri", dummyCreationTime,
                DateUtils.parseDateOozieTZ(actionNominalTime), missDeps, 10, CoordinatorAction.Status.WAITING);

    }

    private void _testGetActionsSubset(String jobId, String actionId, int start, int len, String consoleUrl,
            String errorCode, String errorMessage, String externalId, String externalStatus, String trackerUri,
            Date createdTime, Date nominalTime, String missDeps, int timeout, CoordinatorAction.Status status)
            throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetActionsSubsetJPAExecutor actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(jobId, null, start,
                len, false);
        List<CoordinatorActionBean> actions = jpaService.execute(actionGetCmd);
        CoordinatorActionBean action = actions.get(0);

        assertEquals(1, actions.size());
        assertEquals(actionId, action.getId());
        assertEquals(jobId, action.getJobId());
        assertEquals(consoleUrl, action.getConsoleUrl());
        assertEquals(errorCode, action.getErrorCode());
        assertEquals(errorMessage, action.getErrorMessage());
        assertEquals(externalId, action.getExternalId());
        assertEquals(externalStatus, action.getExternalStatus());
        assertEquals(trackerUri, action.getTrackerUri());
        assertEquals(createdTime, action.getCreatedTime());
        assertEquals(nominalTime, action.getNominalTime());
        assertEquals(missDeps, action.getMissingDependencies());
        assertEquals(timeout, action.getTimeOut());
        assertEquals(status, action.getStatus());
    }

    // Check the ordering of actions by nominal time
    public void testCoordActionOrderBy() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        // Add Coordinator action with nominal time: 2009-12-15T01:00Z
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.WAITING,
                "coord-action-get.xml", 0);
        // Add Coordinator action with nominal time: 2009-02-01T23:59Z
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.WAITING,
                "coord-action-for-action-input-check.xml", 0);
        // test for the expected action number
        List<CoordinatorActionBean> actions = _testGetActionsSubsetOrderBy(job.getId(), 1, 2, false);
        assertEquals(actions.size(), 2);
        // As actions are sorted by nominal time, the first action should be
        // with action number 2
        assertEquals(actions.get(0).getActionNumber(), 2);
    }

    // Check the ordering of actions by nominal time
    public void testCoordActionOrderByDesc() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        // Add Coordinator action with nominal time: 2009-12-15T01:00Z
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.WAITING,
                "coord-action-get.xml", 0);
        // Add Coordinator action with nominal time: 2009-02-01T23:59Z
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.WAITING,
                "coord-action-for-action-input-check.xml", 0);
        // test for the expected action number
        List<CoordinatorActionBean> actions = _testGetActionsSubsetOrderBy(job.getId(), 1, 2, true);
        assertEquals(actions.size(), 2);
        // As actions are sorted by nominal time in desc order, the first action
        // should be with action number 1
        assertEquals(actions.get(0).getActionNumber(), 1);
    }

    private List<CoordinatorActionBean> _testGetActionsSubsetOrderBy(String jobId, int start, int len, boolean order)
            throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetActionsSubsetJPAExecutor actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(jobId, null, start,
                len, order);
        return jpaService.execute(actionGetCmd);
    }

    // Check status filters for Coordinator actions
    public void testCoordActionFilter() throws Exception {
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        // Add Coordinator action with nominal time: 2009-12-15T01:00Z
        addRecordToCoordActionTable(job.getId(), 1, CoordinatorAction.Status.RUNNING, "coord-action-get.xml", 0);
        // Add Coordinator action with nominal time: 2009-02-01T23:59Z
        addRecordToCoordActionTable(job.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0);
        // Create lists for status filter to test positive filter
        Map<Pair<String, FILTER_COMPARATORS>, List<Object>>
            filterMap = new HashMap<Pair<String, FILTER_COMPARATORS>, List<Object>>();
        List<Object> positiveFilter = new ArrayList<Object>();
        positiveFilter.add("RUNNING");
        positiveFilter.add("KILLED");
        filterMap.put(POSITIVE_STATUS_FILTER, positiveFilter);
        List<CoordinatorActionBean> actions = _testGetActionsSubsetFilter(job.getId(), 1, filterMap, 1, 2);
        assertEquals(actions.size(), 1);
        assertEquals(actions.get(0).getActionNumber(), 1);

        // Create lists for status filter to test negative filter
        filterMap.clear();
        List<Object> negativeFilter = new ArrayList<Object>();
        negativeFilter.add("WAITING");
        negativeFilter.add("KILLED");
        filterMap.put(NEGATIVE_STATUS_FILTER, negativeFilter);
        actions = _testGetActionsSubsetFilter(job.getId(), 1, filterMap, 1, 2);
        assertEquals(actions.size(), 1);
        assertEquals(actions.get(0).getActionNumber(), 1);

        // Test Combination of include/exclude filters - no dup
        filterMap.clear();
        filterMap.put(POSITIVE_STATUS_FILTER, positiveFilter);
        filterMap.put(NEGATIVE_STATUS_FILTER, negativeFilter);
        actions = _testGetActionsSubsetFilter(job.getId(), 1, filterMap, 1, 2);
        assertEquals(actions.size(), 1);
        assertEquals(actions.get(0).getActionNumber(), 1);

        // Test Combination of include/exclude filters - dup --> no result
        filterMap.clear();
        filterMap.put(POSITIVE_STATUS_FILTER, positiveFilter);
        filterMap.put(NEGATIVE_STATUS_FILTER, positiveFilter);
        actions = _testGetActionsSubsetFilter(job.getId(), 1, filterMap, 1, 2);
        assertEquals(actions.size(), 0);
    }
    // Check whether actions are retrieved based on the filter values for status
    private List<CoordinatorActionBean> _testGetActionsSubsetFilter(String jobId, int actionNum,
            Map<Pair<String, FILTER_COMPARATORS>, List<Object>> filterMap, int start,
        int len) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetActionsSubsetJPAExecutor actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(jobId, filterMap, start,
                len, false);
        return jpaService.execute(actionGetCmd);
    }

    public void testGetActionAllColumns() throws Exception {
        services.destroy();
        setSystemProperty(CoordActionGetForInfoJPAExecutor.COORD_GET_ALL_COLS_FOR_ACTION, "true");
        services = new Services();
        services.init();
        int actionNum = 1;
        String slaXml = "slaXml";
        String resourceXmlName = "coord-action-get.xml";
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
        CoordinatorActionBean action = createCoordAction(job.getId(), actionNum, CoordinatorAction.Status.WAITING,
                resourceXmlName, 0);
        action.setSlaXml(slaXml);
        insertRecordCoordAction(action);
        _testGetForInfoAllActions(job.getId(), slaXml, 1, 1);
    }

    private void _testGetForInfoAllActions(String jobId, String slaXml, int start, int len) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobGetActionsSubsetJPAExecutor actionGetCmd = new CoordJobGetActionsSubsetJPAExecutor(jobId, null, start,
                len, false);
        List<CoordinatorActionBean> actions = jpaService.execute(actionGetCmd);
        CoordinatorActionBean action = actions.get(0);

        assertEquals(CoordinatorAction.Status.WAITING, action.getStatus());
        assertEquals(slaXml, action.getSlaXml());
        assertEquals(0, action.getPending());
    }

}
