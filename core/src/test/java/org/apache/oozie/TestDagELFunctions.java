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
package org.apache.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.lite.EndNodeDef;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.StartNodeDef;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;

public class TestDagELFunctions extends XTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        services.init();
    }

    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testFunctions() throws Exception {
        XConfiguration conf = new XConfiguration();
        conf.set(OozieClient.APP_PATH, "appPath");
        conf.set(OozieClient.USER_NAME, "user");
        conf.set("a", "A");
        LiteWorkflowApp def =
                new LiteWorkflowApp("name", "<workflow-app/>",
                    new StartNodeDef(LiteWorkflowStoreService.LiteControlNodeHandler.class, "end")).
                        addNode(new EndNodeDef("end", LiteWorkflowStoreService.LiteControlNodeHandler.class));
        LiteWorkflowInstance job = new LiteWorkflowInstance(def, conf, "wfId");

        WorkflowJobBean wf = new WorkflowJobBean();
        wf.setId(job.getId());
        wf.setAppName("name");
        wf.setAppPath("appPath");
        wf.setUser("user");
        wf.setGroup("group");
        wf.setWorkflowInstance(job);
        wf.setRun(2);
        wf.setProtoActionConf(conf.toXmlString());

        WorkflowActionBean action = new WorkflowActionBean();
        action.setId("actionId");
        action.setName("actionName");
        action.setErrorInfo("ec", "em");
        action.setData("b=B");
        action.setExternalId("ext");
        action.setTrackerUri("tracker");
        action.setExternalStatus("externalStatus");

        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("workflow");
        DagELFunctions.configureEvaluator(eval, wf, action);

        assertEquals("wfId", eval.evaluate("${wf:id()}", String.class));
        assertEquals("name", eval.evaluate("${wf:name()}", String.class));
        assertEquals("appPath", eval.evaluate("${wf:appPath()}", String.class));
        assertEquals("A", eval.evaluate("${wf:conf('a')}", String.class));
        assertEquals("A", eval.evaluate("${a}", String.class));
        assertEquals("user", eval.evaluate("${wf:user()}", String.class));
        assertEquals("group", eval.evaluate("${wf:group()}", String.class));
        assertTrue(eval.evaluate("${wf:callback('XX')}", String.class).contains("id=actionId"));
        assertTrue(eval.evaluate("${wf:callback('XX')}", String.class).contains("status=XX"));
        assertTrue(eval.evaluate("${wf:callback('XX')}", String.class).contains("status=XX"));
        assertEquals(2, (int) eval.evaluate("${wf:run()}", Integer.class));

        action.setStatus(WorkflowAction.Status.ERROR);
        System.out.println("WorkflowInstance " + wf.getWorkflowInstance().getStatus().toString());
        WorkflowInstance wfInstance = wf.getWorkflowInstance();
        DagELFunctions.setActionInfo(wfInstance, action);
        wf.setWorkflowInstance(wfInstance);

        assertEquals("actionName", eval.evaluate("${wf:lastErrorNode()}", String.class));
        assertEquals("ec", eval.evaluate("${wf:errorCode('actionName')}", String.class));
        assertEquals("em", eval.evaluate("${wf:errorMessage('actionName')}", String.class));

        assertEquals("B", eval.evaluate("${wf:actionData('actionName')['b']}", String.class));

        String expected = XmlUtils.escapeCharsForXML("{\"b\":\"B\"}");
        assertEquals(expected, eval.evaluate("${toJsonStr(wf:actionData('actionName'))}", String.class));
        expected = XmlUtils.escapeCharsForXML("b=B");
        assertTrue(eval.evaluate("${toPropertiesStr(wf:actionData('actionName'))}", String.class).contains(expected));
        conf = new XConfiguration();
        conf.set("b", "B");
        expected = XmlUtils.escapeCharsForXML(XmlUtils.prettyPrint(conf).toString());
        assertTrue(eval.evaluate("${toConfigurationStr(wf:actionData('actionName'))}", String.class).contains(expected));

        assertEquals("ext", eval.evaluate("${wf:actionExternalId('actionName')}", String.class));
        assertEquals("tracker", eval.evaluate("${wf:actionTrackerUri('actionName')}", String.class));
        assertEquals("externalStatus", eval.evaluate("${wf:actionExternalStatus('actionName')}", String.class));
    }

}
