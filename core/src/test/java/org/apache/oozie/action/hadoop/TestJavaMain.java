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

import org.apache.oozie.util.XConfiguration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class TestJavaMain extends MainTestCase {

    @Override
    public Void call() throws Exception {

        XConfiguration jobConf = new XConfiguration();
        XConfiguration.copy(createJobConf(), jobConf);

        jobConf.set("oozie.action.java.main", LauncherMainTester.class.getName());

        jobConf.set("mapreduce.job.tags", "" + System.currentTimeMillis());
        setSystemProperty("oozie.job.launch.time", "" + System.currentTimeMillis());

        File actionXml = new File(getTestCaseDir(), "action.xml");
        OutputStream os = new FileOutputStream(actionXml);
        jobConf.writeXml(os);
        os.close();

        File newId = new File(getTestCaseDir(), "newId");

        System.setProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());

        // Check normal execution
        JavaMain.main(new String[0]);

        // Check Exception handling
        try {
            JavaMain.main(new String[]{"ex2"});
        } catch(JavaMainException jme) {
            assertTrue(jme.getCause() instanceof IOException);
            assertEquals("throwing exception", jme.getCause().getMessage());
        }

        return null;
    }
}
