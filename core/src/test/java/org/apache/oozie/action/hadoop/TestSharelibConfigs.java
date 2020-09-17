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

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.test.MiniOozieTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TestSharelibConfigs extends MiniOozieTestCase {

    private TestWorkflowHelper helper;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        helper = new TestWorkflowHelper(getJobTrackerUri(), getNameNodeUri(), getTestCaseDir());
    }

    public void testActionSharelibConfigPropagation() throws Exception {
        final String globalConfigForJavaSharelib = "<global><configuration><property>" +
                "<name>oozie.action.sharelib.for.java</name><value>globalJavaConfigSharelib</value>" +
                "</property></configuration></global>";
        final String globalLauncherWithSharelib = "<global><launcher>" +
                "<sharelib>globalLauncherSharelib</sharelib>" +
                "</launcher></global>";
        final String globalLauncherAndConfigWithSharelib = "<global><launcher>" +
                "<sharelib>globalLauncherSharelib</sharelib>" +
                "</launcher><configuration><property>" +
                "<name>oozie.action.sharelib.for.java</name><value>globalJavaConfigSharelib</value>" +
                "</property></configuration></global>";
        final String localConfigForJavaSharelib = "<configuration><property>" +
                "<name>oozie.action.sharelib.for.java</name><value>localJavaConfigSharelib</value>" +
                "</property></configuration>";
        final String localLauncherWithSharelib = "<launcher>" +
                "<sharelib>localLauncherSharelib</sharelib>" +
                "</launcher>";

        launchJavaActionAndValidateSharelibValues("", "",
                null, null);
        launchJavaActionAndValidateSharelibValues(globalConfigForJavaSharelib, "",
                "globalJavaConfigSharelib", null);
        launchJavaActionAndValidateSharelibValues(globalLauncherWithSharelib, "",
                null, "globalLauncherSharelib");
        launchJavaActionAndValidateSharelibValues(globalLauncherAndConfigWithSharelib, localConfigForJavaSharelib,
                "localJavaConfigSharelib", "globalLauncherSharelib");
        launchJavaActionAndValidateSharelibValues(globalLauncherAndConfigWithSharelib, localLauncherWithSharelib,
                "globalJavaConfigSharelib", "localLauncherSharelib");
        launchJavaActionAndValidateSharelibValues(globalLauncherAndConfigWithSharelib,
                localLauncherWithSharelib + localConfigForJavaSharelib,
                "localJavaConfigSharelib", "localLauncherSharelib");

    }

    private void launchJavaActionAndValidateSharelibValues(String globalConfig,
                                                           String localConfig,
                                                           String oozieSharelibForJavaPropertyValue,
                                                           String oozieSharelibPropertyValue)
            throws Exception {
        final String workflowUri = helper.createTestWorkflowXml(globalConfig,
                helper.getJavaActionXml(localConfig));
        final OozieClient wfClient = LocalOozie.getClient();
        final Properties conf = wfClient.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, workflowUri);
        conf.setProperty(OozieClient.USER_NAME, getTestUser());
        conf.setProperty("appName", "var-app-name");
        conf.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "true");
        final String jobId = wfClient.submit(conf);
        wfClient.start(jobId);
        WorkflowJob workflow = wfClient.getJobInfo(jobId);
        waitFor(20 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                WorkflowAction javaAction = helper.getJavaAction(wfClient.getJobInfo(jobId));
                return javaAction != null && !javaAction.getStatus().equals("PREP");
            }
        });
        final XConfiguration actionConf = getJavaActionConfiguration(workflow);
        assertThat("Configuration priorities are incorrect! Global/local configs are not overwriting each other.",
                actionConf.get(LauncherAM.OOZIE_LAUNCHER_SHARELIB_PROPERTY), is(oozieSharelibPropertyValue));
        assertThat("Configuration priorities are incorrect! Global/local configs are not overwriting each other.",
                actionConf.get("oozie.action.sharelib.for.java"), is(oozieSharelibForJavaPropertyValue));
    }

    private XConfiguration getJavaActionConfiguration(WorkflowJob workflow) throws Exception {
        final WorkflowAction workflowAction = helper.getJavaAction(workflow);
        final Element element = XmlUtils.parseXml(workflowAction.getConf());
        final String configuration = XmlUtils.prettyPrint(element.getChild("configuration",
                element.getNamespace())).toString();
        return new XConfiguration(new StringReader(configuration));
    }

}
