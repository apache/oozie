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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.util.XConfiguration;

//Test cases are mainly implemented in the Base class

public class TestShellMain extends ShellTestCase {

    @Override
    public Void call() throws Exception {
        File script = new File(getTestCaseDir(), scriptName);
        Writer w = new FileWriter(script);
        w.write(scriptContent);
        w.close();
        script.setExecutable(true);

        XConfiguration jobConf = new XConfiguration();

        jobConf.set("user.name", getTestUser());
        jobConf.set("group.name", getTestGroup());
        jobConf.setInt("mapred.map.tasks", 1);
        jobConf.setInt("mapred.map.max.attempts", 1);
        jobConf.setInt("mapred.reduce.max.attempts", 1);
        jobConf.set("mapred.job.tracker", getJobTrackerUri());
        jobConf.set("fs.default.name", getNameNodeUri());


        jobConf.set(ShellMain.CONF_OOZIE_SHELL_EXEC, SHELL_COMMAND_NAME);
        String[] args = new String[] { SHELL_COMMAND_SCRIPTFILE_OPTION, script.toString(), "A", "B" };
        MapReduceMain.setStrings(jobConf, ShellMain.CONF_OOZIE_SHELL_ARGS, args);
        MapReduceMain.setStrings(jobConf, ShellMain.CONF_OOZIE_SHELL_ENVS,
                new String[] { "var1=value1", "var2=value2" });

        File actionXml = new File(getTestCaseDir(), "action.xml");
        FileOutputStream os = new FileOutputStream(actionXml);
        jobConf.writeXml(os);
        os.close();

        setSystemProperty("oozie.launcher.job.id", "" + System.currentTimeMillis());
        setSystemProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());

        Properties props = jobConf.toProperties();
        // Test arguments count
        assertEquals(props.getProperty(ShellMain.CONF_OOZIE_SHELL_ARGS + ".size"), "4");
        // Test environment count
        assertEquals(props.getProperty(ShellMain.CONF_OOZIE_SHELL_ENVS + ".size"), "2");

        try {
            ShellMain.main(null);
            if (expectedSuccess == false) {
                fail("Expected to fail");
            }
        }
        catch (LauncherMainException le) {
            if (expectedSuccess == true) {
                fail("Should be successful");
            }
        }

        return null;
    }

}
