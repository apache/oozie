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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase.Context;
import org.apache.oozie.action.hadoop.SharelibUtils;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.jdom.Element;

public class TestGitActionExecutor extends ActionExecutorTestCase{

    @SuppressWarnings("unchecked")
    public void testSetupMethods() throws Exception {
        GitActionExecutor ae = new GitActionExecutor();
        assertTrue("Can not find GitMain class is launcher classes",
          ae.getLauncherClasses().contains(GitMain.class));

        final String repoUrl = "https://github.com/apache/oozie";
        final String keyUrl = "this_would_be_an_HDFS_path";
        final String destDir = "repoDir";
        final String branch = "myBranch";
        Element actionXml = XmlUtils.parseXml("<git>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<git-uri>" + repoUrl + "</git-uri>"+
                "<branch>" + branch + "</branch>"+
                "<key-path>" + keyUrl + "</key-path>"+
                "<destination-uri>" + destDir + "</destination-uri>" +
                "</git>");

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "git-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());

        Context context = new Context(wf, action);

        Configuration conf = ae.createBaseHadoopConf(context, actionXml);

        ae.setupActionConf(conf, context, actionXml, getFsTestCaseDir());
        assertEquals(repoUrl, conf.get(GitActionExecutor.GIT_URI));
        assertEquals(keyUrl, conf.get(GitActionExecutor.KEY_PATH));
        assertEquals(branch, conf.get(GitActionExecutor.GIT_BRANCH));
        assertEquals(destDir, conf.get(GitActionExecutor.DESTINATION_URI));
    }

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

    protected RunningJob submitAction(Context context) throws Exception {
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
        final RunningJob runningJob = jobClient.getJob(JobID.forName(jobId));
        assertNotNull(runningJob);
        return runningJob;
    }

}
