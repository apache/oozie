/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.wf;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.service.XLogService;
import org.jdom.Element;
import org.jdom.Namespace;

import java.io.StringReader;
import java.util.List;
import java.util.Properties;

public class TestSubmitMRCommand extends XFsTestCase {
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
        conf.set("oozie.libpath", "libpath");

        conf.set(XOozieClient.FILES, "/user/oozie/input1.txt,/user/oozie/input2.txt#my.txt");
        conf.set(XOozieClient.ARCHIVES, "/user/oozie/udf1.jar,/user/oozie/udf2.jar#my.jar");

        SubmitMRCommand submitMRCmd = new SubmitMRCommand(conf, "token");
        String xml = submitMRCmd.getWorkflowXml(conf);

        Element gen = XmlUtils.parseXml(xml);
        Namespace ns = gen.getNamespace();
        assertEquals("hadoop1", gen.getChild("start", ns).getAttributeValue("to"));
        assertEquals("hadoop1", gen.getChild("action", ns).getAttributeValue("name"));
        assertEquals("fail", gen.getChild("kill", ns).getAttributeValue("name"));
        assertEquals("end", gen.getChild("end", ns).getAttributeValue("name"));

        assertEquals("jobtracker", gen.getChild("action", ns).getChild("map-reduce", ns).getChildText("job-tracker", ns));
        assertEquals("namenode", gen.getChild("action", ns).getChild("map-reduce", ns).getChildText("name-node", ns));
        Element eConf = gen.getChild("action", ns).getChild("map-reduce", ns).getChild("configuration", ns);
        XConfiguration xConf = new XConfiguration(new StringReader(XmlUtils.prettyPrint(eConf).toString()));
        Properties genProps = xConf.toProperties();
        Properties expectedProps = new Properties();
        expectedProps.setProperty("mapred.mapper.class", "A.Mapper");
        expectedProps.setProperty("mapred.reducer.class", "A.Reducer");
        expectedProps.setProperty("oozie.libpath", "libpath");
        assertEquals(expectedProps, genProps);
        assertEquals("/user/oozie/input1.txt#input1.txt",
                     ((Element)gen.getChild("action", ns).getChild("map-reduce", ns)
                             .getChildren("file", ns).get(0)).getText());
        assertEquals("/user/oozie/input2.txt#my.txt",
                     ((Element)gen.getChild("action", ns).getChild("map-reduce", ns)
                             .getChildren("file", ns).get(1)).getText());
        assertEquals("/user/oozie/udf1.jar#udf1.jar",
                     ((Element)gen.getChild("action", ns).getChild("map-reduce", ns)
                             .getChildren("archive", ns).get(0)).getText());
        assertEquals("/user/oozie/udf2.jar#my.jar",
                     ((Element)gen.getChild("action", ns).getChild("map-reduce", ns)
                             .getChildren("archive", ns).get(1)).getText());
        assertEquals("end", gen.getChild("action", ns).getChild("ok", ns).getAttributeValue("to"));
        assertEquals("fail", gen.getChild("action", ns).getChild("error", ns).getAttributeValue("to"));
        assertNotNull(gen.getChild("kill", ns).getChild("message", ns));        

    }

    public void testWFXmlGenerationWithoutLibPath() throws Exception {
        Configuration conf = new Configuration();

        conf.set(XOozieClient.JT, "jobtracker");
        conf.set(XOozieClient.NN, "namenode");
        // conf.set(XOozieClient.LIBPATH, "libpath");

        conf.set("name_a", "value_a");
        conf.set("name_b", "value_b");
        conf.set("name_c", "value_c");

        SubmitMRCommand submitMRCmd = new SubmitMRCommand(conf, "token");
        submitMRCmd.getWorkflowXml(conf);
    }
}
