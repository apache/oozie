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
package org.apache.oozie.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Date;

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XLogStreamer;


public class TestLogStreamer extends XTestCase {
    public void testStreamLog() throws IOException {
        long currTime = System.currentTimeMillis();
        XLogStreamer.Filter.reset();
        XLogStreamer.Filter.defineParameter("USER");
        XLogStreamer.Filter.defineParameter("GROUP");
        XLogStreamer.Filter.defineParameter("TOKEN");
        XLogStreamer.Filter.defineParameter("APP");
        XLogStreamer.Filter.defineParameter("JOB");
        XLogStreamer.Filter.defineParameter("ACTION");
        XLogStreamer.Filter xf = new XLogStreamer.Filter();
        xf.setParameter("JOB", "14-200904160239--example-forkjoinwf");
        xf.setLogLevel("DEBUG|INFO");

        FileWriter fw1 = new FileWriter(getTestCaseDir() + "/test.log");
        StringBuilder sb1 = new StringBuilder();
        sb1.append("2009-06-24 02:43:13,958 DEBUG _L1_:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-forkjoinwf] JOB[14-200904160239--example-forkjoinwf] ACTION[-] End workflow state change");
        sb1.append("\n2009-06-24 02:43:13,961  INFO _L2_:317 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] JOB[14-200904160239--example-forkjoinwf] ACTION[-] [org.apache.oozie.core.command.WorkflowRunnerCallable] released lock");
        fw1.write(sb1.toString());
        fw1.close();
        File f1 = new File(getTestCaseDir() + "/test.log");
        f1.setLastModified(currTime - 9000);

        FileWriter fw2 = new FileWriter(getTestCaseDir() + "/test.log.1");
        StringBuilder sb2 = new StringBuilder();
        sb2.append("\n2009-06-24 02:43:13,986  WARN _L3_:539 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] JOB[14-200904160239--example-forkjoinwf] ACTION[-] Use GenericOptionsParser for parsing the arguments. \n_L3A_Applications should implement Tool for the same. \n_L3B_Multi line test");
        sb2.append("\n2009-06-24 02:43:14,431  INFO _L4_:661 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] JOB[14-200904160239--example-forkjoinwf] ACTION[-] No job jar file set.  User classes may not be found. See JobConf(Class) or JobConf#setJar(String).");
        fw2.write(sb2.toString());
        fw2.close();
        File f2 = new File(getTestCaseDir() + "/test.log.1");
        f2.setLastModified(currTime - 8000);

        FileWriter fw3 = new FileWriter(getTestCaseDir() + "/test.log.2");
        StringBuilder sb3 = new StringBuilder();
        sb3.append("\n2009-06-24 02:43:14,505  INFO _L5_:317 - USER[oozie] GROUP[oozie] TOKEN[-] APP[-] JOB[-] ACTION[-]  Released Lock");
        sb3.append("\n2009-06-24 02:43:19,344 DEBUG _L6_:323 - USER[oozie] GROUP[oozie] TOKEN[MYtoken] APP[-] JOB[-] ACTION[-] Number of pending signals to check [0]");
        sb3.append("\n2009-06-24 02:43:29,151 DEBUG _L7_:323 - USER[-] GROUP[-] TOKEN[-] APP[-] JOB[14-200904160239--example-forkjoinwf] ACTION[-] Number of pending actions [0] ");
        fw3.write(sb3.toString());
        fw3.close();
        File f3 = new File(getTestCaseDir() + "/test.log.2");
        f3.setLastModified(currTime);

        FileWriter fwerr = new FileWriter(getTestCaseDir() + "/testerr.log");
        StringBuilder sberr = new StringBuilder();
        sberr.append("2009-06-24 02:43:13,958 WARN _L1_:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-forkjoinwf] JOB[14-200904160239--example-forkjoinwf] ACTION[-] End workflow state change");
        sberr.append("\n2009-06-24 02:43:13,961  INFO _L2_:317 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] JOB[14-200904160239--example-forkjoinwf] ACTION[-] [org.apache.oozie.core.command.WorkflowRunnerCallable] released lock");
        fwerr.write(sberr.toString());
        fwerr.close();
        File ferr = new File(getTestCaseDir() + "/testerr.log");
        ferr.setLastModified(currTime - 8000);

        StringWriter sw = new StringWriter();
        XLogStreamer str = new XLogStreamer(xf, sw, getTestCaseDir(), "test.log", 1);
        str.streamLog(new Date(currTime - 10000), new Date(currTime - 5000));
        String[] out = sw.toString().split("\n");
        assertEquals(3, out.length);
        assertEquals(true, out[0].contains("_L1_"));
        assertEquals(true, out[1].contains("_L2_"));
        assertEquals(true, out[2].contains("_L4_"));

        StringWriter sw1 = new StringWriter();
        XLogStreamer str1 = new XLogStreamer(xf, sw1, getTestCaseDir(), "test.log", 1);
        str1.streamLog(null, null);
        out = sw1.toString().split("\n");
        assertEquals(4, out.length);
        assertEquals(true, out[0].contains("_L1_"));
        assertEquals(true, out[1].contains("_L2_"));
        assertEquals(true, out[2].contains("_L4_"));
        assertEquals(true, out[3].contains("_L7_"));
    }
}
