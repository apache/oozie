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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;

public class TestDistCpActionExecutor extends ActionExecutorTestCase{

    @SuppressWarnings("unchecked")
    public void testSetupMethods() throws Exception {
        DistcpActionExecutor ae = new DistcpActionExecutor();
        assertEquals(Arrays.asList(DistcpMain.class), ae.getLauncherClasses());
    }

    public void testDistCpFile() throws Exception {
        Path inputPath = new Path(getFsTestCaseDir(), "input.txt");
        final Path outputPath = new Path(getFsTestCaseDir(), "output.txt");
        byte[] content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes();

        OutputStream os = getFileSystem().create(inputPath);
        os.write(content);
        os.close();

        String actionXml = "<distcp>" +
                "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "<arg>" + inputPath + "</arg>"+
                "<arg>" + outputPath + "</arg>" +
                "</distcp>";
        Context context = createContext(actionXml);
        final String launcherId = submitAction(context);
        waitUntilYarnAppDoneAndAssertSuccess(launcherId);

        waitFor(60 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return getFileSystem().exists(outputPath);
            }
        });
        assertTrue(getFileSystem().exists(outputPath));

        byte[] readContent = new byte[content.length];
        InputStream is = getFileSystem().open(outputPath);
        int offset = 0;
        while (offset < readContent.length)
        {
            int numRead = is.read(readContent, offset, readContent.length);
            if(numRead == -1) {
                break;
            }
            offset += numRead;
        }
        assertEquals(is.read(), -1);
        is.close();
        offset = 0;
        while (offset < readContent.length)
        {
            assertEquals(readContent[offset], content[offset]);
            offset++;
        }

        // Check for external ids
        DistcpActionExecutor ae = new DistcpActionExecutor();
        WorkflowAction wfAction = context.getAction();
        ae.check(context, wfAction);
        ae.end(context, wfAction);

        assertEquals("SUCCEEDED", wfAction.getExternalStatus());
        String externalIds = wfAction.getExternalChildIDs();
        assertNotNull(externalIds);
        assertNotSame("", externalIds);
        // check for the expected prefix of hadoop jobIDs
        assertTrue(externalIds.contains("job_"));

    }

    protected Context createContext(String actionXml) throws Exception {
        DistcpActionExecutor ae = new DistcpActionExecutor();

        Path appJarPath = new Path("lib/test.jar");
        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "test.jar", LauncherMainTester.class);
        InputStream is = new FileInputStream(jarFile);
        OutputStream os = getFileSystem().create(new Path(getAppPath(), "lib/test.jar"));
        IOUtils.copyStream(is, os);

        Path appSoPath = new Path("lib/test.so");
        getFileSystem().create(new Path(getAppPath(), appSoPath)).close();

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        protoConf.setStrings(WorkflowAppService.APP_LIB_PATH_LIST, appJarPath.toString(), appSoPath.toString());


        WorkflowJobBean wf = createBaseWorkflow(protoConf, "action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    protected String submitAction(Context context) throws Exception {
        DistcpActionExecutor ae = new DistcpActionExecutor();

        WorkflowAction action = context.getAction();

        ae.prepareActionDir(getFileSystem(), context);
        ae.submitLauncher(getFileSystem(), context, action);

        String jobId = action.getExternalId();
        String jobTracker = action.getTrackerUri();
        String consoleUrl = action.getConsoleUrl();
        assertNotNull(jobId);
        assertNotNull(jobTracker);
        assertNotNull(consoleUrl);

        ae.submitLauncher(getFileSystem(), context, context.getAction());
        return context.getAction().getExternalId();
    }

}
