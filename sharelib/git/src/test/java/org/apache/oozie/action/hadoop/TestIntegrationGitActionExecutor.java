
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TestIntegrationGitActionExecutor extends ActionExecutorTestCase{

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", GitActionExecutor.class.getName());
    }

    public void testWhenRepoIsClonedThenGitIndexContentIsReadSuccessfully() throws Exception {
        final Path outputPath = getFsTestCaseDir();
        final Path gitRepo = Path.mergePaths(outputPath, new Path("/repoDir"));
        final Path gitIndex = Path.mergePaths(gitRepo, new Path("/.git/config"));

        final GitServer gitServer = new GitServer();

        final String localRepo = String.format("git://127.0.0.1:%s/repo.git", gitServer.getLocalPort());
        final String actionXml = "<git>" +
                "<resource-manager>" + getJobTrackerUri() + "</resource-manager>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<git-uri>" + localRepo + "</git-uri>"+
                "<destination-uri>" + gitRepo + "</destination-uri>" +
                "</git>";

        final Context context = createContext(actionXml);
        final String launcherId = submitAction(context);

        try {
            gitServer.start();

            waitUntilYarnAppDoneAndAssertSuccess(launcherId);
        }
        finally {
            gitServer.stopAndCleanupReposServer();
        }
        final Map<String, String> actionData = LauncherHelper.getActionData(getFileSystem(), context.getActionDir(),
                context.getProtoActionConf());
        assertFalse(LauncherHelper.hasIdSwap(actionData));

        final GitActionExecutor ae = new GitActionExecutor();
        ae.check(context, context.getAction());
        assertEquals("launcherId and action.externalId should be the same", launcherId, context.getAction().getExternalId());
        assertEquals("action should have been SUCCEEDED", "SUCCEEDED", context.getAction().getExternalStatus());

        ae.end(context, context.getAction());
        assertEquals("action.status should be OK", WorkflowAction.Status.OK, context.getAction().getStatus());

        assertTrue("could not create test case output path", getFileSystem().exists(outputPath));
        assertTrue("could not save git index", getFileSystem().exists(gitIndex));

        try (final InputStream is = getFileSystem().open(gitIndex)) {
            final String gitIndexContent = IOUtils.toString(is, StandardCharsets.UTF_8);

            assertTrue("could not read git index", gitIndexContent.toLowerCase().contains("core"));
            assertTrue("could not read git index", gitIndexContent.toLowerCase().contains("remote"));
        }
    }

    private Context createContext(final String actionXml) throws Exception {
        final GitActionExecutor ae = new GitActionExecutor();

        final XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        final FileSystem fs = getFileSystem();
        SharelibUtils.addToDistributedCache(GitActionExecutor.GIT_ACTION_TYPE, fs, getFsTestCaseDir(), protoConf);

        final WorkflowJobBean wf = createBaseWorkflow(protoConf, GitActionExecutor.GIT_ACTION_TYPE + "-action");
        final WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    private String submitAction(final Context context) throws Exception {
        final GitActionExecutor ae = new GitActionExecutor();

        final WorkflowAction action = context.getAction();

        ae.prepareActionDir(getFileSystem(), context);
        ae.submitLauncher(getFileSystem(), context, action);

        final String externalId = action.getExternalId();
        final String trackerUri = action.getTrackerUri();
        final String consoleUrl = action.getConsoleUrl();
        assertNotNull("action.externalId should be filled", externalId);
        assertNotNull("action.trackerUri should be filled", trackerUri);
        assertNotNull("action.consoleUrl should be filled", consoleUrl);

        final Configuration conf = createJobConf();

        final String runningJobExternalId = context.getAction().getExternalId();

        assertNotNull("running job has a valid externalId", runningJobExternalId);

        return runningJobExternalId;
    }

}
