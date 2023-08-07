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

package org.apache.oozie.command.wf;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.apache.oozie.DagEngine;
import org.apache.oozie.ForTestingActionExecutor;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.WorkflowActionsGetForJobJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.ExtendedCallableQueueService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.workflow.lite.LiteWorkflowAppParser;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.service.RecoveryService;



public class TestSignalXCommand extends XDataTestCase {

    private Services services;
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        ConfigurationService.setBoolean(LiteWorkflowAppParser.VALIDATE_FORK_JOIN, false);

    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }
    public void testJoinFail() throws Exception{
        ConfigurationService.setBoolean(SignalXCommand.FORK_PARALLEL_JOBSUBMISSION, true);
        _testJoinFail();
        ConfigurationService.setBoolean(SignalXCommand.FORK_PARALLEL_JOBSUBMISSION, false);
        _testJoinFail();
    }

    public void testSuspendPoints() throws Exception{
        ConfigurationService.setBoolean(SignalXCommand.FORK_PARALLEL_JOBSUBMISSION, true);
        _testSuspendPoints();
        services.destroy();
        services = new Services();
        services.init();
        ConfigurationService.setBoolean(SignalXCommand.FORK_PARALLEL_JOBSUBMISSION, false);
        _testSuspendPoints();
    }

    public void testSuspendPointsAll() throws Exception{
        ConfigurationService.setBoolean(SignalXCommand.FORK_PARALLEL_JOBSUBMISSION, true);
        _testSuspendPointsAll();
        services.destroy();
        services = new Services();
        services.init();
        ConfigurationService.setBoolean(SignalXCommand.FORK_PARALLEL_JOBSUBMISSION, false);
        _testSuspendPointsAll();
    }

    public void _testJoinFail() throws Exception {
        Logger logger = Logger.getLogger(SignalXCommand.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Layout layout = new SimpleLayout();
        Appender appender = new WriterAppender(layout, out);
        logger.addAppender(appender);

        FileSystem fs = getFileSystem();
        Path appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        Reader reader = IOUtils.getResourceAsReader("wf-fork.xml", -1);
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")),
                StandardCharsets.UTF_8);
        IOUtils.copyCharStream(reader, writer);
        writer.close();
        reader.close();

        final DagEngine engine = new DagEngine("u");

        XConfiguration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, new Path(appPath, "workflow.xml").toString());
        conf.set(OozieClient.USER_NAME, getTestUser());

        final String jobId = engine.submitJob(conf, false);


        assertNotNull(jobId);
        engine.start(jobId);

        Thread.sleep(2000);
        assertFalse(out.toString(StandardCharsets.UTF_8.name()).contains("EntityExistsException"));
    }

    public void _testSuspendPoints() throws Exception {
        services.destroy();
        LocalOozie.start();
        FileSystem fs = getFileSystem();
        Path appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        Reader reader = IOUtils.getResourceAsReader("wf-suspendpoints.xml", -1);
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")),
                StandardCharsets.UTF_8);
        IOUtils.copyCharStream(reader, writer);
        writer.close();
        reader.close();

        final OozieClient oc = LocalOozie.getClient();

        Properties conf = oc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, new Path(appPath, "workflow.xml").toString());
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty("oozie.suspend.on.nodes", "action1,nonexistant_action_name,decision1, action3,join1 ,fork1,action4b");

        final String jobId = oc.submit(conf);
        assertNotNull(jobId);

        WorkflowJob wf = oc.getJobInfo(jobId);
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

        long beforeStart = System.currentTimeMillis();

        oc.start(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action1"},
                new String[]{":start:"});

        // Check for creation time
        long afterStart = System.currentTimeMillis();
        WorkflowJob wf1 = oc.getJobInfo(jobId);
        for (WorkflowAction action : wf1.getActions()) {
            WorkflowActionBean bean = (WorkflowActionBean) action;
            assertNotNull(bean.getCreatedTime());
            assertTrue((bean.getCreatedTime().getTime() > beforeStart) && (bean.getCreatedTime().getTime() < afterStart));
        }

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"decision1"},
                new String[]{":start:", "action1", "action2"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action3"},
                new String[]{":start:", "action1", "action2", "decision1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"fork1"},
                new String[]{":start:", "action1", "action2", "decision1", "action3"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action4a", "action4b", "action4c"},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"join1"},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1", "action4a", "action4b", "action4c"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUCCEEDED,
                new String[]{},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1", "action4a", "action4b", "action4c",
                             "join1", "end"});
        LocalOozie.stop();
    }

    public void _testSuspendPointsAll() throws Exception {
        services.destroy();
        LocalOozie.start();
        FileSystem fs = getFileSystem();
        Path appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        Reader reader = IOUtils.getResourceAsReader("wf-suspendpoints.xml", -1);
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")),
                StandardCharsets.UTF_8);
        IOUtils.copyCharStream(reader, writer);
        writer.close();
        reader.close();

        final OozieClient oc = LocalOozie.getClient();

        Properties conf = oc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, new Path(appPath, "workflow.xml").toString());
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty("oozie.suspend.on.nodes", "*");

        final String jobId = oc.submit(conf);
        assertNotNull(jobId);

        WorkflowJob wf = oc.getJobInfo(jobId);
        assertEquals(WorkflowJob.Status.PREP, wf.getStatus());

        oc.start(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action1"},
                new String[]{":start:"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action2"},
                new String[]{":start:", "action1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"decision1"},
                new String[]{":start:", "action1", "action2"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action3"},
                new String[]{":start:", "action1", "action2", "decision1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"fork1"},
                new String[]{":start:", "action1", "action2", "decision1", "action3"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"action4a", "action4b", "action4c"},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"join1"},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1", "action4a", "action4b", "action4c"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUSPENDED,
                new String[]{"end"},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1", "action4a", "action4b", "action4c",
                             "join1"});

        oc.resume(jobId);
        checkSuspendActions(wf, oc, jobId, WorkflowJob.Status.SUCCEEDED,
                new String[]{},
                new String[]{":start:", "action1", "action2", "decision1", "action3", "fork1", "action4a", "action4b", "action4c",
                             "join1", "end"});
        LocalOozie.stop();
    }

    private void checkSuspendActions(WorkflowJob wf, final OozieClient oc, final String jobId, final WorkflowJob.Status status,
            String[] prepActions, String[] okActions) throws Exception {
        // Wait for the WF to transition to status
        waitFor(30 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                WorkflowJob wf = oc.getJobInfo(jobId);
                return wf.getStatus() == status;
            }
        });
        wf = oc.getJobInfo(jobId);
        assertEquals(status, wf.getStatus());

        // Check the actions' statuses
        int numPrep = 0;
        int numOK = 0;
        for (WorkflowAction action : wf.getActions()) {
            boolean checked = false;
            for (String name : prepActions) {
                if (!checked && name.equals(action.getName())) {
                    assertEquals("action [" + action.getName() + "] had incorrect status",
                            WorkflowAction.Status.PREP, action.getStatus());
                    numPrep++;
                    checked = true;
                }
            }
            if (!checked) {
                for (String name : okActions) {
                    if (!checked && name.equals(action.getName())) {
                        assertEquals("action [" + action.getName() + "] had incorrect status",
                                WorkflowAction.Status.OK, action.getStatus());
                    numOK++;
                    checked = true;
                    }
                }
            }
            if (!checked) {
                fail("Unexpected action [" + action.getName() + "] with status [" + action.getStatus() + "]");
            }
        }
        assertEquals(prepActions.length, numPrep);
        assertEquals(okActions.length, numOK);
    }

    private void writeToFile(String appXml, String appPath) throws IOException {
        File wf = new File(URI.create(appPath));
        try(PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(wf), StandardCharsets.UTF_8))) {
            out.println(appXml);
        }
    }

    /*
     *  This test case is just to test possible dead-lock when
     *  the conf of oozie.workflow.parallel.fork.action.start
     *  is enabled.
     *
     *  Details could be linked to OOZIE-3646
     */
    public void testPossibleDeadLock() throws Exception {
        setSystemProperty(Services.CONF_SERVICE_EXT_CLASSES, ExtendedCallableQueueService.class.getName());

        services = new Services();
        Configuration servicesConf = services.getConf();
        servicesConf.setInt(CallableQueueService.CONF_THREADS, 1);
        services.init();

        ConfigurationService.setBoolean(SignalXCommand.FORK_PARALLEL_JOBSUBMISSION, true);

        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
        //@formatter:off
        String appXml = "<workflow-app xmlns=\"uri:oozie:workflow:0.4\" name=\"wf-fork\">"
                + "<start to=\"fork1\"/>"
                + "<fork name=\"fork1\">"
                + "<path start=\"action1\"/>"
                + "<path start=\"action2\"/>"
                + "<path start=\"action3\"/>"
                + "<path start=\"action4\"/>"
                + "<path start=\"action5\"/>"
                + "</fork>"
                + "<action name=\"action1\">"
                + "<fs></fs>"
                + "<ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<action name=\"action2\">"
                + "<fs></fs><ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<action name=\"action3\">"
                + "<fs></fs><ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<action name=\"action4\">"
                + "<fs></fs><ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<action name=\"action5\">"
                + "<fs></fs><ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<join name=\"join1\" to=\"end\"/>"
                + "<kill name=\"kill\"><message>killed</message>"
                + "</kill><"
                + "end name=\"end\"/>"
                + "</workflow-app>";
        //@Formatter:on

        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());

        SubmitXCommand sc = new SubmitXCommand(conf);
        final String jobId = sc.call();
        new StartXCommand(jobId).call();

        waitFor(20 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return WorkflowJobQueryExecutor.getInstance()
                        .get(WorkflowJobQueryExecutor.WorkflowJobQuery.GET_WORKFLOW, jobId)
                        .getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });

        assertEquals(WorkflowJobQueryExecutor.getInstance()
                        .get(WorkflowJobQueryExecutor.WorkflowJobQuery.GET_WORKFLOW, jobId)
                        .getStatus(),
                WorkflowJob.Status.SUCCEEDED);
    }

    /**
     * Test : fork parallel submit, one transition fail, and the job is failed but the other transition
     *        always RUNNING or PREP.
     *        verify the PreconditionException is thrown when action2 = RUNNING or PREP and job = FAIL
     *
     */
    public void testForkParallelSubmitFail() throws Exception {
        _testForkSubmitRunFail(true);
    }

    /**
     * Test : fork serial submit, one transition fail, and the job is failed but the other transition
     *        always RUNNING or PREP.
     *        verify the PreconditionException is thrown when action2 = RUNNING or PREP and job = FAIL
     *
     */
    public void testForkSerialSubmitFail() throws Exception {
        _testForkSubmitRunFail(false);
    }

    /**
     * Test : fork parallel submit, one transition fail, and the job is failed but the other transition
     *        always RUNNING or PREP.
     *        verify the PreconditionException is thrown when action2 = RUNNING or PREP and job = FAIL
     *
     * @param isForkParallelSubmit ("ture" or "fail")
     */
    private void _testForkSubmitRunFail(boolean isForkParallelSubmit) throws Exception {
        services.destroy();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        setSystemProperty(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE_EXT, ForTestingActionExecutor.TEST_ERROR);
        services = new Services();
        services.init();
        services.get(ActionService.class).registerAndInitExecutor(ForTestingActionExecutor.class);
        ConfigurationService.setBoolean(SignalXCommand.FORK_PARALLEL_JOBSUBMISSION, isForkParallelSubmit);

        String workflowUri = getTestCaseFileUri("workflow.xml");
        //@formatter:off
        String appXml = "<workflow-app xmlns=\"uri:oozie:workflow:1.0\" name=\"wf-fork-submit\">\n" +
                "    <start to=\"fork1\"/>\n" +
                "     <fork name=\"fork1\">\n" +
                "        <path start=\"action_to_be_failed\"/>\n" +
                "        <path start=\"action_to_be_succeeded_or_killed\"/>\n" +
                "    </fork>\n" +
                "    <action name=\"action_to_be_failed\">\n" +
                "        <test xmlns=\"uri:test\">\n" +
                "            <signal-value>${wf:conf('signal-value')}</signal-value>\n" +
                "            <external-status>${wf:conf('external-status')}</external-status>\n" +
                "            <error>${wf:conf('error')}</error>\n" +
                "            <avoid-set-execution-data>${wf:conf('avoid-set-execution-data')}</avoid-set-execution-data>\n" +
                "            <avoid-set-end-data>${wf:conf('avoid-set-end-data')}</avoid-set-end-data>\n" +
                "            <running-mode>${wf:conf('running-mode')}</running-mode>\n" +
                "        </test>\n" +
                "        <ok to=\"join1\"/>\n" +
                "        <error to=\"kill\"/>\n" +
                "    </action>\n" +
                "    <action name=\"action_to_be_succeeded_or_killed\">\n" +
                "        <test xmlns=\"uri:test\">\n" +
                "            <signal-value>based_on_action_status</signal-value>\n" +
                "            <external-status>ok</external-status>\n" +
                "            <error>ok</error>\n" +
                "            <avoid-set-execution-data>true</avoid-set-execution-data>\n" +
                "            <avoid-set-end-data>false</avoid-set-end-data>\n" +
                "            <running-mode>async</running-mode>\n" +
                "        </test>\n" +
                "        <ok to=\"join1\"/>\n" +
                "        <error to=\"kill\"/>\n" +
                "    </action>\n" +
                "    <join name=\"join1\" to=\"end\"/>\n" +
                "    <kill name=\"kill\">\n" +
                "        <message>killed</message>\n" +
                "    </kill>\n" +
                "    <end name=\"end\"/>\n" +
                "</workflow-app>";
        //@Formatter:on
        writeToFile(appXml, workflowUri);

        final DagEngine engine = new DagEngine("u");

        Configuration conf = new Configuration();
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.LOG_TOKEN, "t");
        conf.set("error", "start.fail");
        conf.set("external-status", "error");
        conf.set("signal-value", "based_on_action_status");

        final String jobId = engine.submitJob(conf, true);
        final WorkflowActionsGetForJobJPAExecutor actionsGetExecutor = new WorkflowActionsGetForJobJPAExecutor(jobId);
        final JPAService jpaService = Services.get().get(JPAService.class);

        waitFor(30 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return WorkflowJob.Status.FAILED.equals(engine.getJob(jobId).getStatus());
            }
        });

        // wait for execute KillXCommand, and all actions has finished
        waitFor(30 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
                for (WorkflowActionBean action : actions) {
                    if (WorkflowAction.Status.PREP.equals(action.getStatus()) ||
                            WorkflowAction.Status.RUNNING.equals(action.getStatus()) ){
                        return false;
                    }
                }
                return true;
            }
        });

        List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
        assertEquals("action size [" + actions.size() + "] had incorrect", 4, actions.size());

        for (WorkflowActionBean action : actions) {
            if ("action_to_be_failed".equals(action.getName())){
                assertEquals("action [" + action.getName() + "] had incorrect status",
                        WorkflowAction.Status.FAILED, action.getStatus());
            }

            if ("action_to_be_succeeded_or_killed".equals(action.getName())){
                // 1.The "action_to_be_failed" action submit fail, and the "action_to_be_succeeded_or_killed" action
                //   has finished, so the "action_to_be_succeeded_or_killed" action should be OK
                // 2.The "action_to_be_failed" action submit fail, and the "action_to_be_succeeded_or_killed" action is
                //   PREP or RUNNING, so the "action_to_be_succeeded_or_killed" action should be KILLED
                if (!WorkflowAction.Status.KILLED.equals(action.getStatus()) &&
                        !WorkflowAction.Status.OK.equals(action.getStatus())) {
                    fail("Unexpected action [" + action.getName() + "] with status [" + action.getStatus() + "]");
                }
            }
        }
    }

    /**
     * for test {@link #testDeadlockForForkParallelSubmit()}
     */
    public static class TestRecoverForkStartActionCallableQueueService extends CallableQueueService{
        private final XLog log = XLog.getLog(getClass());
        public static TestSignalXCommand testSignalXCommand;

        /**
         *  Overwrite for test the same action's ActionStartXCommand in queue and the ForkedActionStartXCommand wouldn't lose,
         *  if ActionStartXCommand and ForkedActionStartXCommand has the same name, ForkedActionStartXCommand couldn't enqueue
         *  after a ActionStartXCommand in queue waiting for running.
         */
        public class CallableWrapper<E> extends CallableQueueService.CallableWrapper<E> {

            boolean forkedActionStartXCommandFirstEnter;
            public CallableWrapper(XCallable<E> callable, long delay) {
                super(callable,delay);
                forkedActionStartXCommandFirstEnter = callable instanceof ForkedActionStartXCommand;
            }


            public void run() {
                XCallable<?> callable = getElement();
                if (forkedActionStartXCommandFirstEnter && callable instanceof ForkedActionStartXCommand){
                    // make sure there has a ActionStartXCommand in the queue wait to run,
                    // and then ForkedActionStartXCommand enqueue
                    testSignalXCommand.waitFor(15 * 1000, new Predicate() {
                        @Override
                        public boolean evaluate() throws Exception {
                            return !filterDuplicates();
                        }
                    },200);

                    log.warn("max concurrency for callable [{0}] exceeded, enqueueing with [{1}]ms delay", callable
                            .getType(), CONCURRENCY_DELAY);
                    setDelay(CONCURRENCY_DELAY, TimeUnit.MILLISECONDS);

                    try {
                        Method queue = CallableQueueService.class.getDeclaredMethod("queue",
                                CallableQueueService.CallableWrapper.class, boolean.class);
                        queue.setAccessible(true);
                        queue.invoke(TestRecoverForkStartActionCallableQueueService.this,this,true);
                    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    forkedActionStartXCommandFirstEnter = false;
                }else {
                    super.run();
                }
            }
        }

        /**
         * Replace CallableQueueService.CallableWrapper to TestRecoverForkStartActionCallableQueueService.CallableWrapper
         * for text, in order to call TestRecoverForkStartActionCallableQueueService.CallableWrapper for wait
         * ActionStartXCommand enqueue before.
         *
         */
        public <T> Future<T> submit(CallableQueueService.CallableWrapper<T> task) throws InterruptedException {
            return super.submit(new TestRecoverForkStartActionCallableQueueService.CallableWrapper<T>(task.getElement(),
                    task.getInitialDelay()));
        }
    }


    /**
     * Test : fork parallel submit, the action has the same XCommand in queue, there will skip enqueue by
     * {@link CallableQueueService.CallableWrapper#filterDuplicates()} so if the ActionStartXCommand  and
     * ForkedActionStartXCommand has the same name, it would be lost.
     *
     * Note : RecoveryService will check the pending action and try to start it. So if the action's ForkedActionStartXCommand
     * wait for run, there may be a ActionStartXCommand add for the same action.
     *
     */
    public void testDeadlockForForkParallelSubmit() throws Exception {
        setSystemProperty(Services.CONF_SERVICE_EXT_CLASSES, TestRecoverForkStartActionCallableQueueService.class.getName());
        TestRecoverForkStartActionCallableQueueService.testSignalXCommand = this;

        services = new Services();
        Configuration servicesConf = services.getConf();
        servicesConf.setInt(RecoveryService.CONF_WF_ACTIONS_OLDER_THAN, 0);
        servicesConf.setInt(RecoveryService.CONF_SERVICE_INTERVAL, 10);
        services.init();

        ConfigurationService.setBoolean(SignalXCommand.FORK_PARALLEL_JOBSUBMISSION, true);

        Configuration conf = new XConfiguration();
        String workflowUri = getTestCaseFileUri("workflow.xml");
        //@formatter:off
        String appXml = "<workflow-app xmlns=\"uri:oozie:workflow:1.0\" name=\"wf-fork\">"
                + "<start to=\"fork1\"/>"
                + "<fork name=\"fork1\">"
                + "<path start=\"action1\"/>"
                + "<path start=\"action2\"/>"
                + "<path start=\"action3\"/>"
                + "<path start=\"action4\"/>"
                + "<path start=\"action5\"/>"
                + "</fork>"
                + "<action name=\"action1\">"
                + "<fs></fs>"
                + "<ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<action name=\"action2\">"
                + "<fs></fs><ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<action name=\"action3\">"
                + "<fs></fs><ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<action name=\"action4\">"
                + "<fs></fs><ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<action name=\"action5\">"
                + "<fs></fs><ok to=\"join1\"/>"
                + "<error to=\"kill\"/>"
                + "</action>"
                + "<join name=\"join1\" to=\"end\"/>"
                + "<kill name=\"kill\"><message>killed</message>"
                + "</kill><"
                + "end name=\"end\"/>"
                + "</workflow-app>";
        //@Formatter:on

        writeToFile(appXml, workflowUri);
        conf.set(OozieClient.APP_PATH, workflowUri);
        conf.set(OozieClient.USER_NAME, getTestUser());

        SubmitXCommand sc = new SubmitXCommand(conf);
        final String jobId = sc.call();
        new StartXCommand(jobId).call();

        waitFor(30 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return WorkflowJobQueryExecutor.getInstance()
                        .get(WorkflowJobQueryExecutor.WorkflowJobQuery.GET_WORKFLOW, jobId)
                        .getStatus() == WorkflowJob.Status.SUCCEEDED;
            }
        });

        assertEquals(WorkflowJobQueryExecutor.getInstance()
                        .get(WorkflowJobQueryExecutor.WorkflowJobQuery.GET_WORKFLOW, jobId)
                        .getStatus(),
                WorkflowJob.Status.SUCCEEDED);
    }
}
