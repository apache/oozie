package org.apache.oozie.command.coord;

import java.io.IOException;
import java.io.Reader;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.PartitionWrapper;
import org.apache.oozie.util.PartitionsGroup;
import org.apache.oozie.util.XLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCoordActionUpdatePushMissingDependency extends XDataTestCase {
    private String TZ;

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(PartitionDependencyManagerService.HCAT_DEFAULT_SERVER_NAME, "myhcatserver");
        setSystemProperty(PartitionDependencyManagerService.HCAT_DEFAULT_DB_NAME, "myhcatdb");
        setSystemProperty(PartitionDependencyManagerService.MAP_MAX_WEIGHTED_CAPACITY, "100");
        Services services = new Services();
        addServiceToRun(services.getConf(), PartitionDependencyManagerService.class.getName());
        services.init();
        TZ = (getProcessingTZ().equals(DateUtils.OOZIE_PROCESSING_TIMEZONE_DEFAULT)) ? "Z" : getProcessingTZ()
                .substring(3);
    }

    @After
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    @Test
    public void testUpdateCoordTableBasic() throws Exception {
        String newHCatDependency = "hcat://hcat.yahoo.com:5080/mydb/clicks/?datastamp=12&region=us";

        String actionId = addInitRecords(newHCatDependency);
        checkCoordAction(actionId, newHCatDependency, CoordinatorAction.Status.WAITING, 0);

        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);

        pdms.addMissingPartition(newHCatDependency, actionId);

        pdms.partitionAvailable(newHCatDependency);
        HCatURI hcatUri = new HCatURI(newHCatDependency);

        List<PartitionWrapper> availParts = pdms.getAvailablePartitions(actionId);
        assertNotNull(availParts);
        assertEquals(availParts.get(0), new PartitionWrapper(hcatUri));

        new CoordActionUpdatePushMissingDependency(actionId).call();

        checkCoordAction(actionId, "", CoordinatorAction.Status.READY, 0);

    }

    @Test
    public void testUpdateCoordTableAdvanced() throws Exception {
        String newHCatDependency1 = "hcat://hcat.yahoo.com:5080/mydb/clicks/?datastamp=11&region=us";
        String newHCatDependency2 = "hcat://hcat.yahoo.com:5080/mydb/clicks/?datastamp=12&region=us";

        String fullDeps = newHCatDependency1 + CoordELFunctions.DIR_SEPARATOR + newHCatDependency2;
        String actionId = addInitRecords(fullDeps);
        checkCoordAction(actionId, fullDeps, CoordinatorAction.Status.WAITING, 0);

        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);

        pdms.addMissingPartition(newHCatDependency1, actionId);
        pdms.addMissingPartition(newHCatDependency2, actionId);

        pdms.partitionAvailable(newHCatDependency2);
        HCatURI hcatUri2 = new HCatURI(newHCatDependency2);

        List<PartitionWrapper> availParts = pdms.getAvailablePartitions(actionId);
        assertNotNull(availParts);
        assertEquals(availParts.get(0), new PartitionWrapper(hcatUri2));

        new CoordActionUpdatePushMissingDependency(actionId).call();

        checkCoordAction(actionId, newHCatDependency1, CoordinatorAction.Status.WAITING, 1);

        // second partition available

        pdms.partitionAvailable(newHCatDependency1);
        HCatURI hcatUri1 = new HCatURI(newHCatDependency1);

        availParts = pdms.getAvailablePartitions(actionId);
        assertNotNull(availParts);
        assertEquals(availParts.get(0), new PartitionWrapper(hcatUri1));

        new CoordActionUpdatePushMissingDependency(actionId).call();

        checkCoordAction(actionId, "", CoordinatorAction.Status.READY, 0);

    }

    private CoordinatorActionBean checkCoordAction(String actionId, String expDeps, CoordinatorAction.Status stat,
            int type) throws Exception {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            CoordinatorActionBean action = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            String missDeps = action.getPushMissingDependencies();
            if (type != 0) {
                assertEquals(new PartitionWrapper(missDeps), new PartitionWrapper(expDeps));
            }
            else {
                assertEquals(missDeps, expDeps);
            }
            assertEquals(action.getStatus(), stat);

            return action;
        }
        catch (JPAExecutorException se) {
            throw new Exception("Action ID " + actionId + " was not stored properly in db");
        }
    }

    private String addInitRecords(String pushMissingDependencies) throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-02-01T23:59" + TZ);
        Date endTime = DateUtils.parseDateOozieTZ("2009-02-02T23:59" + TZ);
        CoordinatorJobBean job = addRecordToCoordJobTableForWaiting("coord-job-for-action-input-check.xml",
                CoordinatorJob.Status.RUNNING, startTime, endTime, false, true, 3);

        CoordinatorActionBean action1 = addRecordToCoordActionTableForWaiting(job.getId(), 1,
                CoordinatorAction.Status.WAITING, "coord-action-for-action-input-check.xml", pushMissingDependencies);
        return action1.getId();
    }

    protected CoordinatorActionBean addRecordToCoordActionTableForWaiting(String jobId, int actionNum,
            CoordinatorAction.Status status, String resourceXmlName, String pushMissingDependencies) throws Exception {
        CoordinatorActionBean action = createCoordAction(jobId, actionNum, status, resourceXmlName, 0, TZ);
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
            Date start, Date end, boolean pending, boolean doneMatd, int lastActionNum) throws Exception {

        String testDir = getTestCaseDir();
        CoordinatorJobBean coordJob = createCoordJob(testFileName, status, start, end, pending, doneMatd, lastActionNum);
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

    protected String getProcessingTZ() {
        return DateUtils.OOZIE_PROCESSING_TIMEZONE_DEFAULT;
    }
}
