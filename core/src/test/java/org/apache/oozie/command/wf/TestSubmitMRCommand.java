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
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.service.XLogService;
import org.jdom.Element;

public class TestSubmitMRCommand extends XFsTestCase {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty(XLogService.LOG4J_FILE_ENV, "oozie-log4j.properties");
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
        conf.set(XOozieClient.LIBPATH, "libpath");

        conf.set("mapred.mapper.class", "A.Mapper");
        conf.set("mapred.reducer.class", "A.Reducer");

        conf.set(XOozieClient.FILES, "/user/oozie/input1.txt,/user/oozie/input2.txt#my.txt");
        conf.set(XOozieClient.ARCHIVES, "/user/oozie/udf1.jar,/user/oozie/udf2.jar#my.jar");

        SubmitMRCommand submitMRCmd = new SubmitMRCommand(conf, "token");
        String xml = submitMRCmd.getWorkflowXml(conf);

        XLog.getLog(getClass()).info("xml = " + xml);

        StringBuilder sb = new StringBuilder();
        sb.append("<workflow-app xmlns=\"uri:oozie:workflow:0.2\" name=\"oozie-mapreduce\">");
        sb.append("<start to=\"hadoop1\" />");
        sb.append("<action name=\"hadoop1\">");
        sb.append("<map-reduce>");
        sb.append("<job-tracker>jobtracker</job-tracker>");
        sb.append("<name-node>namenode</name-node>");
        sb.append("<configuration>");
        sb.append("<property>");
        sb.append("<name>mapred.mapper.class</name>");
        sb.append("<value>A.Mapper</value>");
        sb.append("</property>");
        sb.append("<property>");
        sb.append("<name>mapred.reducer.class</name>");
        sb.append("<value>A.Reducer</value>");
        sb.append("</property>");
        sb.append("</configuration>");
        sb.append("<file>/user/oozie/input1.txt#input1.txt</file>");
        sb.append("<file>/user/oozie/input2.txt#my.txt</file>");
        sb.append("<archive>/user/oozie/udf1.jar#udf1.jar</archive>");
        sb.append("<archive>/user/oozie/udf2.jar#my.jar</archive>");
        sb.append("</map-reduce>");
        sb.append("<ok to=\"end\" />");
        sb.append("<error to=\"fail\" />");
        sb.append("</action>");
        sb.append("<kill name=\"fail\">");
        sb.append("<message>Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>");
        sb.append("</kill>");
        sb.append("<end name=\"end\" />");
        sb.append("</workflow-app>");

        Element root = XmlUtils.parseXml(sb.toString());
        String reference = XmlUtils.prettyPrint(root).toString();

        XLog.getLog(getClass()).info("reference xml = " + reference);
        Assert.assertTrue(xml.equals(reference));
    }

    public void testWFXmlGenerationNegative1() throws Exception {
        Configuration conf = new Configuration();

        conf.set(XOozieClient.JT, "jobtracker");
        conf.set(XOozieClient.NN, "namenode");
        // conf.set(XOozieClient.LIBPATH, "libpath");

        conf.set("name_a", "value_a");
        conf.set("name_b", "value_b");
        conf.set("name_c", "value_c");

        SubmitMRCommand submitMRCmd = new SubmitMRCommand(conf, "token");
        try {
            submitMRCmd.getWorkflowXml(conf);
            fail("shoud have already failed - missing libpath def");
        }
        catch (Exception e) {

        }
    }
}
