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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.service.XLogService;

public class TestSubmitCommand extends XFsTestCase {
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

    public void testSubmitReservedVars() throws Exception {
        Configuration conf = new XConfiguration();
        String appPath = getTestCaseDir();
        String appXml = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='map-reduce-wf'> "
                + "<start to='end' /> "
                + "<end name='end' /> "
                + "</workflow-app>";

        writeToFile(appXml, appPath);
        conf.set(OozieClient.APP_PATH, appPath);
        conf.set(OozieClient.USER_NAME, getTestUser());
        conf.set(OozieClient.GROUP_NAME, "other");
        conf.set("GB", "5");
        SubmitCommand sc = new SubmitCommand(conf, "UNIT_TESTING");

        try {
            sc.call();
            fail("WF job submission should fail with reserved variable definitions.");
        }
        catch (CommandException ce) {

        }
    }

    private void writeToFile(String appXml, String appPath) throws IOException {
        // TODO Auto-generated method stub
        File wf = new File(appPath + "/workflow.xml");
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(appXml);
        }
        catch (IOException iex) {
            throw iex;
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }
}
