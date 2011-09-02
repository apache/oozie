/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.action.hadoop;

import org.apache.pig.Main;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.net.URL;

public class PigMain extends LauncherMain {

    public static void main(String[] args) throws Exception {
        run(PigMain.class, args);
    }

    protected void run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Oozie Pig action configuration");
        System.out.println("=================================================================");

        // loading action conf prepared by Oozie
        Configuration actionConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");

        if (actionXml == null) {
            throw new RuntimeException("Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file [" + actionXml + "] does not exist");
        }

        actionConf.addResource(new Path("file:///", actionXml));

        Properties props = new Properties();
        for (Map.Entry<String, String> entry : actionConf) {
            props.setProperty(entry.getKey(), entry.getValue());
        }
        OutputStream os = new FileOutputStream("pig.properties");
        props.store(os, "");
        os.close();

        System.out.println();
        System.out.println("/pig.properties content :");
        System.out.println("------------------------");
        props.store(System.out, "");
        System.out.flush();
        System.out.println("------------------------");
        System.out.println();

        List<String> arguments = new ArrayList<String>();
        String script = actionConf.get("oozie.pig.script");

        if (script == null) {
            throw new RuntimeException("Action Configuration does not have [oozie.pig.script] property");
        }

        if (!new File(script).exists()) {
            throw new RuntimeException("Pig script file [" + script + "] does not exist");
        }

        System.out.println("Pig script [" + script + "] content: ");
        System.out.println("------------------------");
        BufferedReader br = new BufferedReader(new FileReader(script));
        String line = br.readLine();
        while (line != null) {
            System.out.println(line);
            line = br.readLine();
        }
        br.close();
        System.out.println("------------------------");
        System.out.println();

        arguments.add("-file");
        arguments.add(script);
        String[] params = MapReduceMain.getStrings(actionConf, "oozie.pig.params");
        for (String param : params) {
            arguments.add("-param");
            arguments.add(param);
        }

        URL log4jFile = Thread.currentThread().getContextClassLoader().getResource("log4j.properties");
        if (log4jFile != null) {

            String pigLogLevel = actionConf.get("oozie.pig.log.level", "INFO");

            // append required PIG properties to the default hadoop log4j file
            Properties hadoopProps = new Properties();
            hadoopProps.load(log4jFile.openStream());
            hadoopProps.setProperty("log4j.logger.org.apache.pig", pigLogLevel + ", A");
            hadoopProps.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
            hadoopProps.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
            hadoopProps.setProperty("log4j.appender.A.layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");

            String localProps = "piglog4j.properties";
            OutputStream os1 = new FileOutputStream(localProps);
            hadoopProps.store(os1, "");
            os1.close();

            // print out current directory
            File localDir = new File(localProps);
            System.out.println("Current dir = " + localDir.getAbsolutePath());

            arguments.add("-log4jconf");
            arguments.add(localProps);
        }
        else {
            System.out.println("log4jfile is null");
        }

        System.out.println("Pig command arguments  :");
        for (String arg : arguments) {
            System.out.println("             " + arg);
        }

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Pig command line now >>>");
        System.out.println();
        System.out.flush();

        runPigJob(arguments.toArray(new String[arguments.size()]));

        System.out.println();
        System.out.println("<<< Invocation of Pig command completed <<<");
        System.out.println();
    }

    protected void runPigJob(String[] args) throws Exception {
        // running as from the command line
        Main.main(args);
    }

    public static void setPigScript(Configuration conf, String script, String[] params) {
        conf.set("oozie.pig.script", script);
        MapReduceMain.setStrings(conf, "oozie.pig.params", params);
    }

}
