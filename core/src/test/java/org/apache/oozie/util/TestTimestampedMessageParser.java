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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.apache.oozie.command.CommandException;
import org.apache.oozie.test.XTestCase;

public class TestTimestampedMessageParser extends XTestCase {

    static File prepareFile1(String dir) throws IOException {
        File file = new File(dir + "/test1.log");
        FileWriter fw = new FileWriter(file);
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
        // a multiline message with a stack trace
        sb.append("\n2013-06-10 10:26:30,202  WARN ActionStartXCommand:542 - USER[rkanter] GROUP[-] TOKEN[] APP[hive-wf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[14-200904160239--example-forkjoinwf@hive-node] Error starting "
                + "action [hive-node]. ErrorType [TRANSIENT], ErrorCode [JA009], Message [JA009: java.io.IOException: Unknown "
                + "protocol to name node: org.apache.hadoop.mapred.JobSubmissionProtocol _L8_\n"
                + "     at org.apache.hadoop.hdfs.server.namenode.NameNode.getProtocolVersion(NameNode.java:156) _L9_\n"
                + "     at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)_L10_\n"
                + "     at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1190) _L11_\n"
                + "     at org.apache.hadoop.ipc.Server$Handler.run(Server.java:1426) _L12_\n"
                + "] _L13_\n"
                + "org.apache.oozie.action.ActionExecutorException: JA009: java.io.IOException: Unknown protocol to name node: "
                + "org.apache.hadoop.mapred.JobSubmissionProtocol _L14_\n"
                + "     at org.apache.hadoop.hdfs.server.namenode.NameNode.getProtocolVersion(NameNode.java:156) _L15_\n"
                + "     at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) _L16_\n"
                + "     at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39) _L17_\n");

        fw.write(sb.toString());
        fw.close();
        return file;
    }

    static File prepareFile2(String dir) throws IOException {
        File file = new File(dir + "/test2.log");
        FileWriter fw = new FileWriter(file);
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
        return file;
    }

    static File prepareFile3(String dir) throws IOException {
        File file = new File(dir + "/test3.log");
        FileWriter fw = new FileWriter(file);

        for (int i = 0; i < 10000; i++) {
            String log = "2009-06-24 02:43:13," + i
                    + " DEBUG _L1_:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                    + "JOB[14-200904160239--found-C] ACTION[14-200904160239--example-C@1] End workflow state change\n";

            fw.write(log);
        }

        fw.close();
        return file;
    }

    public void testNofindLogs() throws CommandException {
        // Test of OOZIE-1691
        XLogFilter.reset();
        XLogFilter.defineParameter("USER");
        XLogFilter.defineParameter("GROUP");
        XLogFilter.defineParameter("TOKEN");
        XLogFilter.defineParameter("APP");
        XLogFilter.defineParameter("JOB");
        XLogFilter.defineParameter("ACTION");
        XLogFilter xf = new XLogFilter();
        xf.setParameter("JOB", "14-200904160239--no-found-C");
        xf.setLogLevel("DEBUG|WARN");
        try {
            File file = prepareFile3(getTestCaseDir());
            StringWriter sw = new StringWriter();
            new TimestampedMessageParser(new BufferedReader(new FileReader(file)), xf).processRemaining(sw, 4096);
            assertTrue(sw.toString().isEmpty());
        }
        catch (Exception e) {
            fail("should not throw Exception");
        }
    }

    public void testProcessRemainingLog() throws IOException, CommandException {

        XLogFilter.reset();
        XLogFilter.defineParameter("USER");
        XLogFilter.defineParameter("GROUP");
        XLogFilter.defineParameter("TOKEN");
        XLogFilter.defineParameter("APP");
        XLogFilter.defineParameter("JOB");
        XLogFilter.defineParameter("ACTION");
        XLogFilter xf = new XLogFilter();
        xf.setParameter("JOB", "14-200904160239--example-forkjoinwf");
        xf.setLogLevel("DEBUG|WARN");
        File file = prepareFile1(getTestCaseDir());
        StringWriter sw = new StringWriter();
        new TimestampedMessageParser(new BufferedReader(new FileReader(file)), xf).processRemaining(sw, 4096);
        String[] out = sw.toString().split("\n");
        assertEquals(14, out.length);
        assertTrue(out[0].contains("_L1_"));
        assertTrue(out[1].contains("_L3_"));
        assertTrue(out[2].contains("_L3A_"));
        assertTrue(out[3].contains("_L3B_"));
        assertTrue(out[4].contains("_L8_"));
        assertTrue(out[5].contains("_L9_"));
        assertTrue(out[6].contains("_L10_"));
        assertTrue(out[7].contains("_L11_"));
        assertTrue(out[8].contains("_L12_"));
        assertTrue(out[9].contains("_L13_"));
        assertTrue(out[10].contains("_L14_"));
        assertTrue(out[11].contains("_L15_"));
        assertTrue(out[12].contains("_L16_"));
        assertTrue(out[13].contains("_L17_"));
    }

    public void testProcessRemainingCoordinatorLogForActions() throws IOException, CommandException {
        XLogFilter.reset();
        XLogFilter.defineParameter("USER");
        XLogFilter.defineParameter("GROUP");
        XLogFilter.defineParameter("TOKEN");
        XLogFilter.defineParameter("APP");
        XLogFilter.defineParameter("JOB");
        XLogFilter.defineParameter("ACTION");
        XLogFilter xf = new XLogFilter();
        xf.setParameter("JOB", "14-200904160239--example-C");
        xf.setParameter("ACTION", "14-200904160239--example-C@1");

        File file = prepareFile2(getTestCaseDir());
        StringWriter sw = new StringWriter();
        new TimestampedMessageParser(new BufferedReader(new FileReader(file)), xf).processRemaining(sw, 4096);
        String[] matches = sw.toString().split("\n");
        assertEquals(2, matches.length);
        assertTrue(matches[0].contains("_L1_"));
        assertTrue(matches[1].contains("_L5_"));
    }

    public void testLifecycle() throws Exception {
        XLogFilter.reset();
        XLogFilter xf = new XLogFilter();
        String str1 = "2009-06-24 02:43:13,958 DEBUG _L1_:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] End workflow state change\n";
        String str2 = "2009-06-24 02:43:13,961  INFO _L2_:317 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-]\n";
        BufferedReader reader = new BufferedReader(new StringReader(str1 + str2));
        TimestampedMessageParser parser = new TimestampedMessageParser(reader, xf);
        assertNull(parser.getLastMessage());
        assertNull(parser.getLastTimestamp());
        assertTrue(parser.increment());
        assertEquals(str1, parser.getLastMessage());
        assertEquals("2009-06-24 02:43:13,958", parser.getLastTimestamp());
        assertTrue(parser.increment());
        assertEquals(str2, parser.getLastMessage());
        assertEquals("2009-06-24 02:43:13,961", parser.getLastTimestamp());
        assertFalse(parser.increment());
        assertEquals(str2, parser.getLastMessage());
        assertEquals("2009-06-24 02:43:13,961", parser.getLastTimestamp());
        parser.closeReader();
    }
}
