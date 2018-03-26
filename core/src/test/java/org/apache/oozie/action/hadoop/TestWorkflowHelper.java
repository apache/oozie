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

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.test.MiniOozieTestCase;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;

public class TestWorkflowHelper {

    private final String jobTrackerUri;
    private final String nameNodeUri;
    private final String testCaseDir;


    public TestWorkflowHelper(String jobTrackerUri, String nameNodeUri, String testCaseDir) {
        this.jobTrackerUri = jobTrackerUri;
        this.nameNodeUri = nameNodeUri;
        this.testCaseDir = testCaseDir;
    }

    protected String getJavaActionXml(String configToAdd) {
        return "<java>" +
                "<job-tracker>" + jobTrackerUri + "</job-tracker>" +
                "<name-node>" + nameNodeUri + "</name-node>" +
                configToAdd +
                "<main-class>MAIN-CLASS</main-class>" +
                "</java>";
    }

    protected String createTestWorkflowXml(final String globalXml, final String actionXml) throws IOException {
        String workflowUri = getTestCaseFileUri("workflow.xml");
        String appXml = "<workflow-app xmlns=\"uri:oozie:workflow:1.0\" name=\"workflow\">" +
                globalXml +
                "<start to=\"java\"/>" +
                "<action name=\"java\">" +
                  actionXml +
                "     <ok to=\"end\"/>" +
                "     <error to=\"fail\"/>" +
                "</action>" +
                "<kill name=\"fail\">" +
                "     <message>Sub workflow failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>" +
                "</kill>" +
                "<end name=\"end\"/>" +
                "</workflow-app>";
        final File f = new File(URI.create(workflowUri));
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(appXml.getBytes("UTF-8"));
        IOUtils.copyStream(inputStream, new FileOutputStream(f));
        return workflowUri;
    }

    protected WorkflowAction getJavaAction(WorkflowJob workflowJob){
        List<WorkflowAction> actions = workflowJob.getActions();
        for(WorkflowAction wa : actions){
            if(wa.getType().equals("java")){
                return wa;
            }
        }
        return null;
    }
    /**
     * Return the URI for a test file. The returned value is the testDir + concatenated URI.
     *
     * @return the test working directory path, it is always an absolute path and appends the relative path. The
     * reason for the manual parsing instead of an actual File.toURI is because Oozie tests use tokens ${}
     * frequently. Something like URI("c:/temp/${HOUR}").toString() will generate escaped values that will break tests
     */
    private String getTestCaseFileUri(String relativeUri) {
        String uri = new File(testCaseDir).toURI().toString();

        // truncates '/' if the testCaseDir was provided with a fullpath ended with separator
        if (uri.endsWith("/")){
            uri = uri.substring(0, uri.length() -1);
        }

        return uri + "/" + relativeUri;
    }

}
