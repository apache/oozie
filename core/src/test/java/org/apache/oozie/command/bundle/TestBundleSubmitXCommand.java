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

package org.apache.oozie.command.bundle;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor;
import org.apache.oozie.executor.jpa.BundleActionQueryExecutor.BundleActionQuery;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.XConfiguration;

public class TestBundleSubmitXCommand extends XDataTestCase {

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

    /**
     * https://issues.apache.org/jira/browse/OOZIE-945
     * 
     * @throws Exception
     */
    public void testJobXmlCommentRemoved() throws Exception {
        // this retrieves bundle-submit-job.xml
        BundleJobBean job = this.addRecordToBundleJobTable(Job.Status.PREP, false);

        final JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);

        Configuration jobConf = null;
        try {
            jobConf = new XConfiguration(new StringReader(job.getConf()));
        }
        catch (IOException ioe) {
            log.warn("Configuration parse error. read from DB :" + job.getConf(), ioe);
            throw new CommandException(ErrorCode.E1005, ioe);
        }

        Path appPath = new Path(jobConf.get(OozieClient.BUNDLE_APP_PATH), "bundle.xml");
        jobConf.set(OozieClient.BUNDLE_APP_PATH, appPath.toString());

        BundleSubmitXCommand command = new BundleSubmitXCommand(true, jobConf);
        BundleJobBean bundleBean = (BundleJobBean) command.getJob();
        bundleBean.setStartTime(new Date());
        bundleBean.setEndTime(new Date());
        command.call();

        // result includes bundle-submit-job.xml file instead of jobId since this is a dryRun mode
        String result = command.submit();
        // bundle-submit-job.xml contains the Apache license but this result should not contain the comment block
        assertTrue("submit result should not contain <!-- ", !result.contains("<!--"));
        assertTrue("submit result should not contain --> ", !result.contains("-->"));

    }

    public void testCoordJobNameParameterization() throws Exception {
        final XConfiguration jobConf = setUpBundle();
        jobConf.set("coordName1", "coord1");
        jobConf.set("coordName2", "coord2");

        BundleSubmitXCommand command = new BundleSubmitXCommand(jobConf);
        final BundleJobBean bundleBean = (BundleJobBean) command.getJob();
        bundleBean.setStartTime(new Date());
        bundleBean.setEndTime(new Date());
        final String jobId = command.call();
        sleep(2000);
        new BundleStartXCommand(jobId).call();
        waitFor(200000, new Predicate() {
            public boolean evaluate() throws Exception {
                List<BundleActionBean> actions = BundleActionQueryExecutor.getInstance().getList(
                        BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, jobId);
                return actions.get(0).getStatus().equals(Job.Status.RUNNING);
            }
        });

        final List<BundleActionBean> actions = BundleActionQueryExecutor.getInstance().getList(
                BundleActionQuery.GET_BUNDLE_ACTIONS_STATUS_UNIGNORED_FOR_BUNDLE, jobId);
        assertEquals(actions.get(0).getCoordName(), "coord1");
        assertEquals(actions.get(1).getCoordName(), "coord2");
    }

    public void testDuplicateCoordName() throws Exception {
        final XConfiguration jobConf = setUpBundle();
        // setting same coordname.
        jobConf.set("coordName1", "coord");
        jobConf.set("coordName2", "coord");

        BundleSubmitXCommand command = new BundleSubmitXCommand(true, jobConf);
        BundleJobBean bundleBean = (BundleJobBean) command.getJob();
        bundleBean.setStartTime(new Date());
        bundleBean.setEndTime(new Date());
        try {
            command.call();
        }
        catch (CommandException e) {
            assertTrue(e.getMessage().contains("Bundle Job submission Error"));
            assertEquals(e.getErrorCode(), ErrorCode.E1310);
        }
    }

    private XConfiguration setUpBundle() throws UnsupportedEncodingException, IOException {
        XConfiguration jobConf = new XConfiguration();

        final Path coordPath1 = new Path(getFsTestCaseDir(), "coord1");
        final Path coordPath2 = new Path(getFsTestCaseDir(), "coord2");
        writeCoordXml(coordPath1, "coord-job-bundle.xml");
        writeCoordXml(coordPath2, "coord-job-bundle.xml");

        Path bundleAppPath = new Path(getFsTestCaseDir(), "bundle");
        String bundleAppXml = getBundleXml("bundle-submit-job.xml");
        assertNotNull(bundleAppXml);
        assertTrue(bundleAppXml.length() > 0);

        bundleAppXml = bundleAppXml.replaceAll("#app_path1",
                Matcher.quoteReplacement(new Path(coordPath1.toString(), "coordinator.xml").toString()));
        bundleAppXml = bundleAppXml.replaceAll("#app_path2",
                Matcher.quoteReplacement(new Path(coordPath2.toString(), "coordinator.xml").toString()));

        writeToFile(bundleAppXml, bundleAppPath, "bundle.xml");
        final Path appPath = new Path(bundleAppPath, "bundle.xml");
        jobConf.set(OozieClient.BUNDLE_APP_PATH, appPath.toString());
        jobConf.set("appName", "test");

        jobConf.set(OozieClient.USER_NAME, getTestUser());
        return jobConf;

    }
}
