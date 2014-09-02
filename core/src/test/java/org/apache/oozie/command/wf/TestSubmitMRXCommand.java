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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.service.XLogService;
import org.jdom.Element;
import org.jdom.Namespace;

import java.util.HashMap;
import java.util.Map;

public class TestSubmitMRXCommand extends XFsTestCase {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        super.tearDown();
    }

    public void testWFXmlGeneration() throws Exception {
        Configuration conf = new Configuration(false);

        conf.set(XOozieClient.JT, "jobtracker");
        conf.set(XOozieClient.NN, "namenode");
        conf.set(OozieClient.LIBPATH, "libpath");

        conf.set("mapred.mapper.class", "A.Mapper");
        conf.set("mapred.reducer.class", "A.Reducer");

        conf.set(XOozieClient.FILES, "/user/oozie/input1.txt,/user/oozie/input2.txt#my.txt");
        conf.set(XOozieClient.ARCHIVES, "/user/oozie/udf1.jar,/user/oozie/udf2.jar#my.jar");

        SubmitMRXCommand submitMRCmd = new SubmitMRXCommand(conf);
        String xml = submitMRCmd.getWorkflowXml(conf);

        XLog.getLog(getClass()).info("xml = " + xml);

        //verifying is a valid WF
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        wps.parseDef(xml, conf);

        Element wfE = XmlUtils.parseXml(xml);
        Namespace ns = wfE.getNamespace();
        Element actionE = wfE.getChild("action", ns).getChild("map-reduce", ns);
        Element confE = actionE.getChild("configuration", ns);
        Map<String, String> props = new HashMap<String, String>();
        for (Object prop : confE.getChildren("property", ns)) {
            Element propE = (Element) prop;
            String name = propE.getChild("name", ns).getTextTrim();
            String value = propE.getChild("value", ns).getTextTrim();
            props.put(name, value);
        }
        Map<String, String> expected = new HashMap<String, String>();
        expected.put("mapred.mapper.class", "A.Mapper");
        expected.put("mapred.reducer.class", "A.Reducer");
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), expected.get(entry.getKey()));
        }
        assertEquals("/user/oozie/input1.txt#input1.txt", ((Element)actionE.getChildren("file", ns).get(0)).getTextTrim());
        assertEquals("/user/oozie/input2.txt#my.txt", ((Element)actionE.getChildren("file", ns).get(1)).getTextTrim());
        assertEquals("/user/oozie/udf1.jar#udf1.jar", ((Element)actionE.getChildren("archive", ns).get(0)).getTextTrim());
        assertEquals("/user/oozie/udf2.jar#my.jar", ((Element)actionE.getChildren("archive", ns).get(1)).getTextTrim());

    }

    public void testWFXmlGenerationNegative1() throws Exception {
        Configuration conf = new Configuration();

        conf.set(XOozieClient.JT, "jobtracker");
        conf.set(XOozieClient.NN, "namenode");
        // conf.set(XOozieClient.LIBPATH, "libpath");

        conf.set("name_a", "value_a");
        conf.set("name_b", "value_b");
        conf.set("name_c", "value_c");

        SubmitMRXCommand submitMRCmd = new SubmitMRXCommand(conf);
        try {
            submitMRCmd.getWorkflowXml(conf);
            fail("shoud have already failed - missing libpath def");
        }
        catch (Exception e) {

        }
    }

    public void testWFXmlGenerationNewConfigProps() throws Exception {
        try {
            Configuration conf = new Configuration(false);
            conf.set(XOozieClient.NN_2, "new_NN");
            conf.set(XOozieClient.JT_2, "new_JT");
            conf.set("mapred.mapper.class", "TestMapper");
            conf.set("mapred.reducer.class", "TestReducer");
            conf.set("mapred.input.dir", "testInput");
            conf.set("mapred.output.dir", "testOutput");
            conf.set(OozieClient.LIBPATH, "libpath");
            conf.set("mapreduce.job.user.name", "test_user");

            SubmitMRXCommand submitMRCmd = new SubmitMRXCommand(conf);
            String xml = submitMRCmd.getWorkflowXml(conf);

            //verifying is a valid WF
            WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
            wps.parseDef(xml, conf);

            Element wfE = XmlUtils.parseXml(xml);
            Namespace ns = wfE.getNamespace();
            Element actionE = wfE.getChild("action", ns).getChild("map-reduce", ns);
            Element nnE = actionE.getChild("name-node", ns);
            assertEquals(nnE.getTextTrim(), "new_NN");
            Element jtE = actionE.getChild("job-tracker", ns);
            assertEquals(jtE.getTextTrim(), "new_JT");
        }
        catch(Exception e) {
            fail("should have passed");
        }
    }
}
