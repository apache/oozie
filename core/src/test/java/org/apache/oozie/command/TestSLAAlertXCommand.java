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

package org.apache.oozie.command;

import java.io.StringReader;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.AppType;
import org.apache.oozie.BaseEngineException;
import org.apache.oozie.BundleEngine;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.CoordJobQueryExecutor.CoordJobQuery;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLACalcStatus;
import org.apache.oozie.sla.SLACalculatorMemory;
import org.apache.oozie.sla.SLAOperations;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XConfiguration;

public class TestSLAAlertXCommand extends XDataTestCase {
    private Services services;
    SLACalculatorMemory slaCalcMemory;
    BundleJobBean bundle;
    CoordinatorJobBean coord1, coord2;
    final BundleEngine bundleEngine = new BundleEngine("u");
    Date startTime;
    final Date endTime = new Date(System.currentTimeMillis() + 1 * 1 * 3600 * 1000);
    final int timeInSec = 60 * 1000;
    final String data = "2014-01-01T00:00Z";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, "org.apache.oozie.service.EventHandlerService,"
                + "org.apache.oozie.sla.service.SLAService");
        conf.setInt(SLAService.CONF_SLA_CHECK_INTERVAL, 600);
        services.init();

    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    public void testBundleSLAAlertCommands() throws Exception {
        setupSLAJobs();
        String jobIdsStr = bundle.getId();
        String actions = "1,2";
        String coords = null;
        bundleEngine.disableSLAAlert(jobIdsStr, actions, null, coords);
        checkSLAStatus(coord1.getId() + "@1", true);
        checkSLAStatus(coord1.getId() + "@2", true);
        checkSLAStatus(coord1.getId() + "@3", false);
        checkSLAStatus(coord1.getId() + "@5", false);
        checkSLAStatus(coord1.getId() + "@4", false);
        checkSLAStatus(coord2.getId() + "@1", true);
        checkSLAStatus(coord2.getId() + "@1", true);

        bundleEngine.enableSLAAlert(jobIdsStr, null, null, null);
        checkSLAStatus(coord1.getId() + "@1", false);
        checkSLAStatus(coord1.getId() + "@2", false);
        checkSLAStatus(coord1.getId() + "@3", false);
        checkSLAStatus(coord1.getId() + "@5", false);
        checkSLAStatus(coord1.getId() + "@4", false);
        checkSLAStatus(coord2.getId() + "@1", false);
        checkSLAStatus(coord2.getId() + "@2", false);

        CoordinatorJobBean job1 = CoordJobQueryExecutor.getInstance().get(
                CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB, coord1.getId());
        XConfiguration xConf = new XConfiguration(new StringReader(job1.getConf()));
        assertEquals(xConf.get(OozieClient.SLA_DISABLE_ALERT), null);

        CoordinatorJobBean job2 = CoordJobQueryExecutor.getInstance().get(
                CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB, coord2.getId());
        xConf = new XConfiguration(new StringReader(job2.getConf()));
        assertEquals(xConf.get(OozieClient.SLA_DISABLE_ALERT), null);

        bundleEngine.disableSLAAlert(jobIdsStr, null, null, "coord1");
        checkSLAStatus(coord1.getId() + "@1", true);
        checkSLAStatus(coord1.getId() + "@2", true);
        checkSLAStatus(coord1.getId() + "@3", true);
        checkSLAStatus(coord1.getId() + "@4", true);
        checkSLAStatus(coord1.getId() + "@5", true);
        checkSLAStatus(coord2.getId() + "@1", false);
        checkSLAStatus(coord2.getId() + "@2", false);

        job1 = CoordJobQueryExecutor.getInstance().get(CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB,
                coord1.getId());
        xConf = new XConfiguration(new StringReader(job1.getConf()));
        assertEquals(xConf.get(OozieClient.SLA_DISABLE_ALERT), SLAOperations.ALL_VALUE);
        bundleEngine.disableSLAAlert(jobIdsStr, null, null, "coord2");
        // with multiple coordID.

        String dates = "2014-01-01T00:00Z::2014-01-03T00:00Z";
        bundleEngine.enableSLAAlert(jobIdsStr, null, dates, "coord1," + coord2.getId());
        checkSLAStatus(coord1.getId() + "@1", false);
        checkSLAStatus(coord1.getId() + "@2", false);
        checkSLAStatus(coord1.getId() + "@3", false);
        checkSLAStatus(coord1.getId() + "@4", true);
        checkSLAStatus(coord1.getId() + "@5", true);
        checkSLAStatus(coord2.getId() + "@1", false);
        checkSLAStatus(coord2.getId() + "@2", false);
        checkSLAStatus(coord2.getId() + "@3", false);
        checkSLAStatus(coord2.getId() + "@4", true);

        try {
            bundleEngine.disableSLAAlert(jobIdsStr, null, null, "dummy");
            fail("Should throw Exception");
        }
        catch (BaseEngineException e) {
            assertEquals(e.getErrorCode(), ErrorCode.E1026);
        }

    }

    public void testSLAChangeCommand() throws Exception {
        setupSLAJobs();
        String newParams = RestConstants.SLA_SHOULD_END + "=10";
        String jobIdsStr = bundle.getId();
        String coords = coord1.getAppName();
        bundleEngine.changeSLA(jobIdsStr, null, null, coords, newParams);

        assertEquals(getSLACalcStatus(coord1.getId() + "@1").getExpectedEnd().getTime(),
                getSLACalcStatus(coord1.getId() + "@1").getNominalTime().getTime() + 10 * timeInSec);
        assertEquals(getSLACalcStatus(coord1.getId() + "@2").getExpectedEnd().getTime(),
                getSLACalcStatus(coord1.getId() + "@2").getNominalTime().getTime() + 10 * timeInSec);

        assertEquals(getSLACalcStatus(coord1.getId() + "@5").getExpectedEnd().getTime(),
                getSLACalcStatus(coord1.getId() + "@5").getNominalTime().getTime() + 10 * timeInSec);
        newParams = "non-valid-param=10";
        try {
            bundleEngine.changeSLA(jobIdsStr, null, null, coords, newParams);
            fail("Should throw Exception");
        }
        catch (BaseEngineException e) {
            assertEquals(e.getErrorCode(), ErrorCode.E1027);
        }
        try {
            new CoordinatorEngine().changeSLA(coord1.getId(), null, null, null, newParams);
            fail("Should throw Exception");
        }
        catch (BaseEngineException e) {
            assertEquals(e.getErrorCode(), ErrorCode.E1027);
        }
    }

    public void testCoordSLAAlertCommands() throws Exception {
        setupSLAJobs();

        final CoordinatorEngine engine = new CoordinatorEngine("u");
        String jobIdsStr = coord1.getId();
        String actions = "1-3,5";
        String coords = null;
        engine.disableSLAAlert(jobIdsStr, actions, null, coords);
        checkSLAStatus(coord1.getId() + "@1", true);
        checkSLAStatus(coord1.getId() + "@2", true);
        checkSLAStatus(coord1.getId() + "@3", true);
        checkSLAStatus(coord1.getId() + "@5", true);
        checkSLAStatus(coord1.getId() + "@4", false);

        actions = "1-3";
        engine.enableSLAAlert(jobIdsStr, actions, null, null);
        checkSLAStatus(coord1.getId() + "@1", false);
        checkSLAStatus(coord1.getId() + "@2", false);
        checkSLAStatus(coord1.getId() + "@3", false);
        checkSLAStatus(coord1.getId() + "@5", true);
        checkSLAStatus(coord1.getId() + "@4", false);

        engine.enableSLAAlert(jobIdsStr, null, null, null);
        checkSLAStatus(coord1.getId() + "@1", false);
        checkSLAStatus(coord1.getId() + "@2", false);
        checkSLAStatus(coord1.getId() + "@3", false);
        checkSLAStatus(coord1.getId() + "@5", false);
        checkSLAStatus(coord1.getId() + "@4", false);
        CoordinatorJobBean job = CoordJobQueryExecutor.getInstance().get(
                CoordJobQueryExecutor.CoordJobQuery.GET_COORD_JOB, jobIdsStr);
        XConfiguration xConf = new XConfiguration(new StringReader(job.getConf()));
        assertEquals(xConf.get(OozieClient.SLA_DISABLE_ALERT), null);

    }

    private void setupSLAJobs() throws Exception {

        coord1 = addRecordToCoordJobTable(Job.Status.RUNNING, true, false);
        Date nominalTime1 = DateUtils.parseDateUTC(data);
        addRecordToCoordActionTable(coord1.getId(), 1, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 1,
                nominalTime1);
        Date nominalTime2 = org.apache.commons.lang.time.DateUtils.addDays(nominalTime1, 1);

        addRecordToCoordActionTable(coord1.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 1,
                nominalTime2);

        Date nominalTime3 = org.apache.commons.lang.time.DateUtils.addDays(nominalTime1, 2);
        addRecordToCoordActionTable(coord1.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 1,
                nominalTime3);

        Date nominalTime4 = org.apache.commons.lang.time.DateUtils.addDays(nominalTime1, 3);
        addRecordToCoordActionTable(coord1.getId(), 4, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 1,
                nominalTime4);
        Date nominalTime5 = org.apache.commons.lang.time.DateUtils.addDays(nominalTime1, 4);
        addRecordToCoordActionTable(coord1.getId(), 5, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 1,
                nominalTime5);

        coord2 = addRecordToCoordJobTable(Job.Status.RUNNING, true, false);
        addRecordToCoordActionTable(coord2.getId(), 1, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0,
                nominalTime1);
        addRecordToCoordActionTable(coord2.getId(), 2, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0,
                nominalTime2);
        addRecordToCoordActionTable(coord2.getId(), 3, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0,
                nominalTime3);
        addRecordToCoordActionTable(coord2.getId(), 4, CoordinatorAction.Status.WAITING, "coord-action-get.xml", 0,
                nominalTime4);

        bundle = addRecordToBundleJobTable(Job.Status.RUNNING, true);
        coord1.setBundleId(bundle.getId());
        coord1.setAppName("coord1");
        coord1.setStartTime(nominalTime1);
        coord1.setMatThrottling(12);
        coord1.setLastActionNumber(5);
        coord2.setBundleId(bundle.getId());
        coord2.setAppName("coord2");
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coord1);
        CoordJobQueryExecutor.getInstance().executeUpdate(CoordJobQuery.UPDATE_COORD_JOB, coord2);
        registerSLABean(coord1.getId(), AppType.COORDINATOR_JOB, null, null);
        registerSLABean(coord2.getId(), AppType.COORDINATOR_JOB, null, null);
        registerSLABean(coord1.getId() + "@1", AppType.COORDINATOR_ACTION, coord1.getId(), nominalTime1);
        registerSLABean(coord1.getId() + "@2", AppType.COORDINATOR_ACTION, coord1.getId(), nominalTime2);
        registerSLABean(coord1.getId() + "@3", AppType.COORDINATOR_ACTION, coord1.getId(), nominalTime3);
        registerSLABean(coord1.getId() + "@4", AppType.COORDINATOR_ACTION, coord1.getId(), nominalTime4);
        registerSLABean(coord1.getId() + "@5", AppType.COORDINATOR_ACTION, coord1.getId(), nominalTime5);
        registerSLABean(coord2.getId() + "@1", AppType.COORDINATOR_ACTION, coord2.getId(), nominalTime1);
        registerSLABean(coord2.getId() + "@2", AppType.COORDINATOR_ACTION, coord2.getId(), nominalTime2);
        registerSLABean(coord2.getId() + "@3", AppType.COORDINATOR_ACTION, coord2.getId(), nominalTime3);
        registerSLABean(coord2.getId() + "@4", AppType.COORDINATOR_ACTION, coord2.getId(), nominalTime4);

        checkSLAStatus(coord1.getId() + "@1", false);
        checkSLAStatus(coord1.getId() + "@2", false);
        checkSLAStatus(coord1.getId() + "@3", false);
        checkSLAStatus(coord1.getId() + "@5", false);
        checkSLAStatus(coord1.getId() + "@4", false);
    }

    private void registerSLABean(String jobId, AppType appType, String parentId, Date nominalTime) throws Exception {
        SLARegistrationBean slaRegBean = new SLARegistrationBean();
        slaRegBean.setNominalTime(nominalTime);
        slaRegBean.setId(jobId);
        slaRegBean.setAppType(appType);
        startTime = new Date(System.currentTimeMillis() - 1 * 1 * 3600 * 1000); // 1 hour back
        slaRegBean.setExpectedStart(startTime);
        slaRegBean.setExpectedDuration(3600 * 1000);
        slaRegBean.setParentId(parentId);
        slaRegBean.setExpectedEnd(endTime); // 1 hour ahead
        Services.get().get(SLAService.class).addRegistrationEvent(slaRegBean);
    }

    private void checkSLAStatus(String id, boolean status) throws JPAExecutorException {
        assertEquals(getSLACalcStatus(id).getSLAConfigMap().containsKey(OozieClient.SLA_DISABLE_ALERT), status);
    }

    private SLACalcStatus getSLACalcStatus(String jobId) throws JPAExecutorException {
        return Services.get().get(SLAService.class).getSLACalculator().get(jobId);

    }
}
