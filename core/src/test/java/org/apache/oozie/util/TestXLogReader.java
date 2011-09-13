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
package org.apache.oozie.util;

import org.apache.oozie.util.XLogStreamer;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.oozie.test.XTestCase;

public class TestXLogReader extends XTestCase {
    public void testProcessLog() throws IOException {
        XLogStreamer.Filter.reset();
        XLogStreamer.Filter.defineParameter("USER");
        XLogStreamer.Filter.defineParameter("GROUP");
        XLogStreamer.Filter.defineParameter("TOKEN");
        XLogStreamer.Filter.defineParameter("APP");
        XLogStreamer.Filter.defineParameter("JOB");
        XLogStreamer.Filter.defineParameter("ACTION");
        XLogStreamer.Filter xf = new XLogStreamer.Filter();
        xf.setParameter("JOB", "14-200904160239--example-forkjoinwf");
        xf.setLogLevel("DEBUG|WARN");

        FileWriter fw = new FileWriter(getTestCaseDir() + "/test.log");
        StringBuilder sb = new StringBuilder();
        sb.append("2009-06-24 02:43:13,958 DEBUG _L1_:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] End workflow state change");
        sb.append("\n2009-06-24 02:43:13,961  INFO _L2_:317 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] "
                + "[org.apache.oozie.core.command.WorkflowRunnerCallable] " + "released lock");
        sb.append("\n2009-06-24 02:43:13,986  WARN _L3_:539 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] Use GenericOptionsParser for parsing "
                + "the arguments. " + "\n_L3A_Applications should implement Tool for the same. \n_L3B_Multi line test");
        sb.append("\n2009-06-24 02:43:14,431  WARN _L4_:661 - No job jar file set.  User classes may not be found. "
                + "See JobConf(Class) or JobConf#setJar(String).");
        sb.append("\n2009-06-24 02:43:14,505  INFO _L5_:317 - USER[oozie] GROUP[oozie] TOKEN[-] APP[-] JOB[-] "
                + "ACTION[-] " + "Released Lock");
        sb.append("\n2009-06-24 02:43:19,344 DEBUG _L6_:323 - USER[oozie] GROUP[oozie] TOKEN[MYtoken] APP[-] "
                + "JOB[-] ACTION[-] Number of pending signals to check [0]");
        sb.append("\n2009-06-24 02:43:29,151 DEBUG _L7_:323 - USER[-] GROUP[-] TOKEN[-] APP[-] JOB[-] ACTION[-] "
                + "Number of pending actions [0] ");

        fw.write(sb.toString());
        fw.close();
        StringWriter sw = new StringWriter();
        XLogReader lr = new XLogReader(new FileInputStream(getTestCaseDir() + "/test.log"), xf, sw);
        lr.processLog();
        String[] out = sw.toString().split("\n");
        assertEquals(4, out.length);
        assertEquals(true, out[0].contains("_L1_"));
        assertEquals(true, out[1].contains("_L3_"));
        assertEquals(true, out[2].contains("_L3A_"));
        assertEquals(true, out[3].contains("_L3B_"));
    }

    public void testProcessCoordinatorLogForActions() throws IOException {
        XLogStreamer.Filter.reset();
        XLogStreamer.Filter.defineParameter("USER");
        XLogStreamer.Filter.defineParameter("GROUP");
        XLogStreamer.Filter.defineParameter("TOKEN");
        XLogStreamer.Filter.defineParameter("APP");
        XLogStreamer.Filter.defineParameter("JOB");
        XLogStreamer.Filter.defineParameter("ACTION");
        XLogStreamer.Filter xf = new XLogStreamer.Filter();
        xf.setParameter("JOB", "14-200904160239--example-C");
        xf.setParameter("ACTION", "14-200904160239--example-C@1");
        FileWriter fw = new FileWriter(getTestCaseDir() + "/test.log");
        StringBuilder sb = new StringBuilder();
        sb.append("2009-06-24 02:43:13,958 DEBUG _L1_:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-C] ACTION[14-200904160239--example-C@1] End workflow state change");
        sb.append("\n2009-06-24 02:43:13,961  INFO _L2_:317 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-C] ACTION[14-200904160239--example-C@2] "
                + "[org.apache.oozie.core.command.WorkflowRunnerCallable] released lock");
        sb.append("\n2009-06-24 02:43:13,986  WARN _L3_:539 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-C] ACTION[14-200904160239--example-C@2] Use GenericOptionsParser for "
                + "parsing the arguments. \n_L3A_Applications should implement Tool for the same. \n_L3B_Multi line "
                + "test");
        sb.append("\n2009-06-24 02:43:14,431  WARN _L4_:661 - No job jar file set.  User classes may not be found. "
                + "See JobConf(Class) or JobConf#setJar(String).");
        sb.append("\n2009-06-24 02:43:14,505  INFO _L5_:317 - USER[oozie] GROUP[oozie] TOKEN[-] APP[-] "
                + "JOB[14-200904160239--example-C] ACTION[14-200904160239--example-C@1] Released Lock");
        sb.append("\n2009-06-24 02:43:19,344 DEBUG _L6_:323 - USER[oozie] GROUP[oozie] TOKEN[MYtoken] APP[-] "
                + "JOB[-] ACTION[-] Number of pending signals to check [0]");
        sb.append("\n2009-06-24 02:43:29,151 DEBUG _L7_:323 - USER[-] GROUP[-] TOKEN[-] APP[-] JOB[-] "
                + "ACTION[-] Number of pending actions [0] ");
        fw.write(sb.toString());
        fw.close();
        StringWriter sw = new StringWriter();
        XLogReader lr = new XLogReader(new FileInputStream(getTestCaseDir() + "/test.log"), xf, sw);
        lr.processLog();
        String[] matches = sw.toString().split("\n");
        assertEquals(2, matches.length);
        assertEquals(true, matches[0].contains("_L1_"));
        assertEquals(true, matches[1].contains("_L5_"));
    }
}
