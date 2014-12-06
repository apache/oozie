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

package org.apache.oozie.action.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.MapReduceActionExecutor;
import org.apache.oozie.action.hadoop.MapperReducerForTest;
import org.apache.oozie.action.hadoop.OozieJobInfo;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.bundle.BundleStartXCommand;
import org.apache.oozie.command.bundle.BundleSubmitXCommand;
import org.apache.oozie.command.wf.ActionXCommand;
import org.apache.oozie.command.wf.ActionXCommand.ActionExecutorContext;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionsGetForJobJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobsGetFromCoordParentIdJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;

public class TestOozieJobInfo extends XDataTestCase {

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

    public void testInfoWithBundle() throws Exception {

        Services.get().getConf().setBoolean(OozieJobInfo.CONF_JOB_INFO, true);
        OozieJobInfo.setJobInfo(true);
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);
        final JPAService jpaService = Services.get().get(JPAService.class);
        Configuration jobConf = null;
        try {
            jobConf = new XConfiguration(new StringReader(job.getConf()));
        }
        catch (IOException ioe) {
            log.warn("Configuration parse error. read from DB :" + job.getConf(), ioe);
            throw new CommandException(ErrorCode.E1005, ioe);
        }
        setCoordConf(jobConf);
        Path appPath = new Path(jobConf.get(OozieClient.BUNDLE_APP_PATH), "bundle.xml");
        jobConf.set(OozieClient.BUNDLE_APP_PATH, appPath.toString());
        BundleSubmitXCommand submitCmd = new BundleSubmitXCommand(jobConf);
        submitCmd.call();
        BundleJobGetJPAExecutor bundleJobGetExecutor = new BundleJobGetJPAExecutor(submitCmd.getJob().getId());
        job = jpaService.execute(bundleJobGetExecutor);

        assertEquals(job.getStatus(), Job.Status.PREP);
        new BundleStartXCommand(job.getId()).call();
        sleep(2000);
        List<BundleActionBean> actions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, job.getId());
        assertEquals(1, actions.size());
        final String bundleID = job.getId();
        waitFor(200000, new Predicate() {
            public boolean evaluate() throws Exception {
                List<BundleActionBean> actions = BundleActionQueryExecutor.getInstance().getList(
                        BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, bundleID);
                return actions.get(0).getStatus().equals(Job.Status.RUNNING);
            }
        });

        actions = BundleActionQueryExecutor.getInstance().getList(BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE,
                job.getId());
        final String cordID = actions.get(0).getCoordId();
        waitFor(200000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordJobGetJPAExecutor coordGetCmd2 = new CoordJobGetJPAExecutor(cordID);
                CoordinatorJobBean cc = jpaService.execute(coordGetCmd2);
                return cc.getStatus().equals(Job.Status.RUNNING);

            }
        });

        final String jobID = jpaService.execute(new WorkflowJobsGetFromCoordParentIdJPAExecutor(cordID, 1)).get(0);
        final WorkflowActionsGetForJobJPAExecutor actionsGetExecutor = new WorkflowActionsGetForJobJPAExecutor(jobID);
        waitFor(200000, new Predicate() {
            public boolean evaluate() throws Exception {
                List<WorkflowActionBean> actions = jpaService.execute(actionsGetExecutor);
                WorkflowActionBean action = null;
                for (WorkflowActionBean bean : actions) {
                    if (bean.getName().contains("hadoop")) {
                        action = bean;
                        break;
                    }
                }
                return action.getStatus().toString().equalsIgnoreCase(Job.Status.RUNNING.toString());
            }
        });

        final WorkflowJobGetJPAExecutor wfeExc = new WorkflowJobGetJPAExecutor(jobID);

        WorkflowJobBean wfbean = jpaService.execute(wfeExc);

        List<WorkflowActionBean> actionList = jpaService.execute(actionsGetExecutor);

        ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(wfbean, actionList.get(1), false,
                false);
        MapReduceActionExecutor actionExecutor = new MapReduceActionExecutor();
        JobConf conf = actionExecutor.createBaseHadoopConf(context, XmlUtils.parseXml(actionList.get(1).getConf()));
        String user = conf.get("user.name");
        JobClient jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, conf);
        String launcherId = actionList.get(1).getExternalId();

        final RunningJob launcherJob = jobClient.getJob(JobID.forName(launcherId));
        FileSystem fs = context.getAppFileSystem();
        Configuration jobXmlConf = new XConfiguration(fs.open(new Path(launcherJob.getJobFile())));
        String jobInfo = jobXmlConf.get(OozieJobInfo.JOB_INFO_KEY);

        // BUNDLE_ID;BUNDLE_NAME;COORDNITOR_NAME;COORDNITOR_NOMINAL_TIME;WORKFLOW_ID;WORKFLOW_NAME;
        // ACTION_TYPE;ACTION_NAME,JOB_INFO,custom_info;
        assertEquals(jobInfo.split(OozieJobInfo.SEPARATOR).length, 11);
        assertTrue(jobInfo.contains(bundleID));
        assertTrue(jobInfo.contains("bundle.name=test_bundle,"));
        assertTrue(jobInfo.contains(cordID));
        assertTrue(jobInfo.contains("action.type=map-reduce"));
        assertTrue(jobInfo.contains(",testing=test,"));
        assertTrue(jobInfo.contains(",coord.nominal.time="));
        assertTrue(jobInfo.contains("launcher=true"));

    }

    protected void setCoordConf(Configuration jobConf) throws IOException {
        Path wfAppPath = new Path(getFsTestCaseDir(), "app");
        FileSystem fs = getFileSystem();
        fs.mkdirs(new Path(wfAppPath, "lib"));
        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "test.jar", MapperReducerForTest.class);
        InputStream is = new FileInputStream(jarFile);
        OutputStream os = fs.create(new Path(wfAppPath, "lib/test.jar"));
        IOUtils.copyStream(is, os);
        Path input = new Path(wfAppPath, "input");
        fs.mkdirs(input);
        Writer writer = new OutputStreamWriter(fs.create(new Path(input, "test.txt")));
        writer.write("hello");
        writer.close();

        final String APP1 = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='app'>" + "<start to='hadoop'/>"
                + "<action name=\"hadoop\">" + "<map-reduce>" + "<job-tracker>${jobTracker}</job-tracker>"
                + "<name-node>${nameNode}</name-node>" + "<configuration>"
                + "<property><name>mapred.map.tasks</name><value>1</value></property>"
                + "<property><name>mapred.reduce.tasks</name><value>0</value></property>"
                + "<property><name>oozie.job.info.testing</name><value>test</value></property>" + "</configuration>"
                + "</map-reduce>" + "<ok to=\"end\"/>" + "<error to=\"k\"/>" + "</action>" + "<kill name=\"k\">"
                + "<message>kill</message>" + "</kill><end name=\"end\"/>" + "</workflow-app>";
        Writer writer2 = new OutputStreamWriter(fs.create(new Path(wfAppPath, "workflow.xml")));
        writer2.write(APP1);
        writer2.close();
        jobConf.set(OozieClient.USER_NAME, getTestUser());
        jobConf.set("myJobTracker", getJobTrackerUri());
        jobConf.set("myNameNode", getNameNodeUri());
        jobConf.set("wfAppPath", new Path(wfAppPath, "workflow.xml").toString());
        jobConf.set("mrclass", MapperReducerForTest.class.getName());
    }

    protected BundleJobBean createBundleJob(Job.Status jobStatus, boolean pending) throws Exception {
        Path coordPath1 = new Path(getFsTestCaseDir(), "coord1");

        String coord = "<coordinator-app name='COORD-TEST' frequency='${coord:days(1)}' "
                + "start=\"${START_TIME}\" end=\"${END_TIME}\" timezone=\"UTC\" "
                + "xmlns=\"uri:oozie:coordinator:0.2\">" + "<controls>" + "<concurrency>2</concurrency>"
                + "<execution>LIFO</execution>" + "</controls>" + "<action>" + "<workflow>"
                + "<app-path>${wfAppPath}</app-path>" + "<configuration>" + "<property>" + "<name>inputA</name>"
                + "<value>aaaa</value>" + "</property>" + "</configuration>" + "</workflow> " + "</action>"
                + "</coordinator-app>";

        writeToFile(coord, coordPath1, "coordinator.xml");

        Path bundleAppPath = new Path(getFsTestCaseDir(), "bundle");
        String bundleAppXml = "<bundle-app name='test_bundle' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
                + "xmlns='uri:oozie:bundle:0.1'> "
                + "<controls> <kick-off-time>2009-02-02T00:00Z</kick-off-time> </controls> "
                + "<coordinator name='c12'> "
                + "<app-path>#app_path1</app-path>"
                + "<configuration> "
                + "<property> <name>START_TIME</name> <value>2009-02-01T00:00Z</value> </property> </configuration> "
                + "</coordinator></bundle-app>";

        bundleAppXml = bundleAppXml.replaceAll("#app_path1",
                Matcher.quoteReplacement(new Path(coordPath1.toString(), "coordinator.xml").toString()));

        writeToFile(bundleAppXml, bundleAppPath, "bundle.xml");

        Configuration conf = new XConfiguration();
        conf.set(OozieClient.BUNDLE_APP_PATH, bundleAppPath.toString());
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set("jobTracker", getJobTrackerUri());
        conf.set("nameNode", getNameNodeUri());
        conf.set("appName", "bundle-app-name");
        conf.set("start", "2009-02-01T00:00Z");
        conf.set("end", "2009-02-01T00:00Z");

        conf.set("START_TIME", "2009-02-01T00:00Z");
        conf.set("END_TIME", "2009-03-01T00:00Z");

        setCoordConf(conf);

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
        if (pending) {
            bundle.setPending();
        }
        else {
            bundle.resetPending();
        }
        bundle.setStatus(jobStatus);
        bundle.setUser(conf.get(OozieClient.USER_NAME));
        return bundle;
    }
}
