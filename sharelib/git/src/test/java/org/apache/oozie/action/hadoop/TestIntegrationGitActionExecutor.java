
/** * Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase.Context;
import org.apache.oozie.action.hadoop.LauncherHelper;
import org.apache.oozie.action.hadoop.SharelibUtils;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.test.XTestCase.Predicate;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.jdom.Element;

public class TestIntegrationGitActionExecutor extends ActionExecutorTestCase{

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", GitActionExecutor.class.getName());
    }

    private static List<String> getAllFilePath(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
        List<String> fileList = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        return fileList;
    }

    /*
     * Test gitting a repo and ensure the data pulled down is a plausible Git repo to
     * ensure that all works as expected
     */
    public void testGitFile() throws Exception {
        final Path outputPath = getFsTestCaseDir();
        final Path gitRepo = Path.mergePaths(outputPath, new Path("/repoDir"));
        final Path gitIndex = Path.mergePaths(gitRepo, new Path("/.git/config"));

        // Oozie is a big repository -- this may timeout, until we can do a shallow clone
        String repoUrl = "https://github.com/apache/oozie";
        String actionXml = "<git>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<git-uri>" + repoUrl + "</git-uri>"+
                "<destination-uri>" + gitRepo + "</destination-uri>" +
                "</git>";

        Context context = createContext(actionXml);
        final String launcherId = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(launcherId);
        Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                context.getProtoActionConf());
        assertFalse(LauncherHelper.hasIdSwap(actionData));

        GitActionExecutor ae = new GitActionExecutor();
        ae.check(context, context.getAction());
        assertTrue(launcherId.equals(context.getAction().getExternalId()));
        assertEquals("SUCCEEDED", context.getAction().getExternalStatus());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        assertTrue(getFileSystem().exists(outputPath));
        assertTrue(getFileSystem().exists(gitIndex));

        // the Git conf index should easily fit in memory
        ByteArrayOutputStream readContent = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        InputStream is = getFileSystem().open(gitIndex);
        int length;
        while ((length = is.read(buffer)) != -1) {
            readContent.write(buffer, 0, length);
        }
        String gitIndexContent = readContent.toString("UTF-8");
        is.close();

        assertTrue(gitIndexContent.toLowerCase().contains("core"));
        assertTrue(gitIndexContent.toLowerCase().contains("remote"));
    }

    private Context createContext(String actionXml) throws Exception {
        GitActionExecutor ae = new GitActionExecutor();

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        FileSystem fs = getFileSystem();
        SharelibUtils.addToDistributedCache("git", fs, getFsTestCaseDir(), protoConf);

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "git-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    protected String submitAction(Context context) throws Exception {
        GitActionExecutor ae = new GitActionExecutor();

        WorkflowAction action = context.getAction();

        ae.prepareActionDir(getFileSystem(), context);
        ae.submitLauncher(getFileSystem(), context, action);

        String jobId = action.getExternalId();
        String jobTracker = action.getTrackerUri();
        String consoleUrl = action.getConsoleUrl();
        assertNotNull(jobId);
        assertNotNull(jobTracker);
        assertNotNull(consoleUrl);

        JobConf jobConf = createJobConf();
        jobConf.set("mapred.job.tracker", jobTracker);

        JobClient jobClient = createJobClient();
        String runningJob = context.getAction().getExternalId();

        assertNotNull(runningJob);
        return runningJob;
    }

}
