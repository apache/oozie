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
import org.apache.oozie.action.hadoop.MapReduceMain;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.service.XLogService;
import org.jdom.Element;

public class TestSubmitSqoopXCommand extends XFsTestCase {
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
        Configuration conf = new Configuration();

        conf.set(XOozieClient.JT, "jobtracker");
        conf.set(XOozieClient.NN, "namenode");
        conf.set(OozieClient.LIBPATH, "libpath");

        conf.set(XOozieClient.SQOOP_COMMAND, "import\n--connect\njdbc:mysql://localhost:3306/oozie");

        String sqoopArgsStr = "-Da=aaa -Db=bbb";
        String[] args = sqoopArgsStr.split(" ");
        MapReduceMain.setStrings(conf, XOozieClient.SQOOP_OPTIONS, args);

        SubmitSqoopXCommand submitSqoopCmd = new SubmitSqoopXCommand(conf);
        String xml = submitSqoopCmd.getWorkflowXml(conf);

        XLog.getLog(getClass()).info("xml = " + xml);

        StringBuilder sb = new StringBuilder();
        sb.append("<workflow-app xmlns=\"uri:oozie:workflow:0.2\" name=\"oozie-sqoop\">");
        sb.append("<start to=\"sqoop1\" />");
        sb.append("<action name=\"sqoop1\">");
        sb.append("<sqoop xmlns=\"uri:oozie:sqoop-action:0.4\">");
        sb.append("<job-tracker>jobtracker</job-tracker>");
        sb.append("<name-node>namenode</name-node>");
        sb.append("<configuration>");
        sb.append("<property>");
        sb.append("<name>a</name>");
        sb.append("<value>aaa</value>");
        sb.append("</property>");
        sb.append("<property>");
        sb.append("<name>b</name>");
        sb.append("<value>bbb</value>");
        sb.append("</property>");
        sb.append("</configuration>");
        sb.append("<arg>import</arg>");
        sb.append("<arg>--connect</arg>");
        sb.append("<arg>jdbc:mysql://localhost:3306/oozie</arg>");
        sb.append("</sqoop>");
        sb.append("<ok to=\"end\" />");
        sb.append("<error to=\"fail\" />");
        sb.append("</action>");
        sb.append("<kill name=\"fail\">");
        sb.append("<message>sqoop failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>");
        sb.append("</kill>");
        sb.append("<end name=\"end\" />");
        sb.append("</workflow-app>");

        Element root = XmlUtils.parseXml(sb.toString());
        String reference = XmlUtils.prettyPrint(root).toString();

        XLog.getLog(getClass()).info("reference xml = " + reference);
        assertTrue(xml.equals(reference));
    }
}
