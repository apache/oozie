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

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.zip.GZIPOutputStream;

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

        // This file will be included in the list of files for log retrieval, provided the modification time lies
        // between the start and end times of the job
        FileWriter fw1 = new FileWriter(getTestCaseDir() + "/oozie.log");
        StringBuilder sb1 = new StringBuilder();
        sb1.append("2009-06-24 02:43:13,958 DEBUG _L1_:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] End workflow state change");
        sb1.append("\n2009-06-24 02:43:13,961 INFO _L2_:317 - USER[-] GROUP[-] TOKEN[-] " + "APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] "
                + "[org.apache.oozie.core.command.WorkflowRunnerCallable] " + "released lock");
        fw1.write(sb1.toString());
        fw1.close();
        File f1 = new File(getTestCaseDir() + "/oozie.log");
        f1.setLastModified(currTime - 9000);

        // This file will be included in the list of files for log retrieval, provided the modification time lies
        // between the start and end times of the job
        FileWriter fw2 = new FileWriter(getTestCaseDir() + "/oozie.log.1");
        StringBuilder sb2 = new StringBuilder();
        sb2.append("\n2009-06-24 02:43:13,986 WARN _L3_:539 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] Use GenericOptionsParser for parsing " + "the "
                + "arguments. " + "\n" + "_L3A_Applications "
                + "should implement Tool for the same. \n_L3B_Multi line test");
        sb2.append("\n2009-06-24 02:43:14,431 INFO _L4_:661 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] No job jar file set. User classes "
                + "may not be found. " + "See JobConf(Class) or JobConf#setJar(String).");
        fw2.write(sb2.toString());
        fw2.close();
        File f2 = new File(getTestCaseDir() + "/oozie.log.1");
        f2.setLastModified(currTime - 8000);

        // This file will be included in the list of files for log retrieval, provided, the modification time lies
        // between the start and end times of the job
        FileWriter fw3 = new FileWriter(getTestCaseDir() + "/oozie.log.2");
        StringBuilder sb3 = new StringBuilder();
        sb3.append("\n2009-06-24 02:43:14,505 INFO _L5_:317 - USER[oozie] GROUP[oozie] TOKEN[-] APP[-] JOB[-] "
                + "ACTION[-] Released Lock");
        sb3.append("\n2009-06-24 02:43:19,344 DEBUG _L6_:323 - USER[oozie] GROUP[oozie] TOKEN[MYtoken] APP[-] JOB[-] "
                + "ACTION[-] Number of pending signals to check [0]");
        sb3.append("\n2009-06-24 02:43:29,151 DEBUG _L7_:323 - USER[-] GROUP[-] TOKEN[-] APP[-] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] Number of pending actions [0] ");
        fw3.write(sb3.toString());
        fw3.close();
        File f3 = new File(getTestCaseDir() + "/oozie.log.2");
        f3.setLastModified(currTime);

        // This file will not be included in the list of files for log retrieval, since the file name neither is equal
        // to nor does begin with the log file pattern specified in log4j properties file. The default value is
        // "oozie.log"
        FileWriter fwerr = new FileWriter(getTestCaseDir() + "/testerr.log");
        StringBuilder sberr = new StringBuilder();
        sberr.append("2009-06-24 02:43:13,958 WARN _L1_:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] End workflow state change");
        sberr.append("\n2009-06-24 02:43:13,961 INFO _L2_:317 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] "
                + "[org.apache.oozie.core.command.WorkflowRunnerCallable] " + "released lock");
        fwerr.write(sberr.toString());
        fwerr.close();
        File ferr = new File(getTestCaseDir() + "/testerr.log");
        ferr.setLastModified(currTime - 8000);

        // This GZip file would be included in list of files for log retrieval, provided, there is an overlap between
        // the two time windows i) time duration during which the GZipped log file is modified ii) time window between
        // start and end times of the job
        Calendar cal = new GregorianCalendar();
        String outFilename = "oozie.log." + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-"
                + cal.get(Calendar.DATE) + "-" + cal.get(Calendar.HOUR_OF_DAY) + ".gz";
        File f = new File(getTestCaseDir() + "/" + outFilename);
        GZIPOutputStream gzout = new GZIPOutputStream(new FileOutputStream(f));
        StringBuilder sb = new StringBuilder();
        sb.append("\n2009-06-24 02:43:13,958 DEBUG _L8_:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-"
                + "forkjoinwf] " + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] End workflow state change");
        sb.append("\n2009-06-24 02:43:13,961 INFO _L9_:317 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] [org.apache.oozie.core."
                + "command.WorkflowRunnerCallable] " + "released lock");
        String strg = sb.toString();
        byte[] buf = strg.getBytes();
        gzout.write(buf, 0, buf.length);
        gzout.finish();
        gzout.close();

        // oozie.log.gz GZip file would always be included in list of files for log retrieval
        outFilename = "oozie.log.gz";
        f = new File(getTestCaseDir() + "/" + outFilename);
        gzout = new GZIPOutputStream(new FileOutputStream(f));
        // Generate and write log content to the GZip file
        sb = new StringBuilder();
        sb.append("\n2009-06-24 02:43:13,958 DEBUG _L10_:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-"
                + "forkjoinwf] " + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] End workflow state change");
        sb.append("\n2009-06-24 02:43:13,961 INFO _L11_:317 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] [org.apache.oozie.core."
                + "command.WorkflowRunnerCallable] " + "released lock");
        strg = sb.toString();
        buf = strg.getBytes();
        gzout.write(buf, 0, buf.length);
        gzout.finish();
        gzout.close();

        // Test to check if an invalid GZip file(file name not in the expected format oozie.log-YYYY-MM-DD-HH.gz) is
        // excluded from log retrieval
        outFilename = "oozie.log-2011-12-03-15.bz2.gz";
        f = new File(getTestCaseDir() + "/" + outFilename);
        gzout = new GZIPOutputStream(new FileOutputStream(f));
        // Generate and write log content to the GZip file
        sb = new StringBuilder();
        sb.append("\n2009-06-24 02:43:13,958 DEBUG _L12_:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-"
                + "forkjoinwf] " + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] End workflow state change");
        sb.append("\n2009-06-24 02:43:13,961 INFO _L13_:317 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] "
                + "JOB[14-200904160239--example-forkjoinwf] ACTION[-] [org.apache.oozie.core."
                + "command.WorkflowRunnerCallable] " + "released lock");
        strg = sb.toString();
        buf = strg.getBytes();
        gzout.write(buf, 0, buf.length);
        gzout.finish();
        gzout.close();

        // Test for the log retrieval of the job that began 10 seconds before and ended 5 seconds before current time
        // respectively
        StringWriter sw = new StringWriter();
        XLogStreamer str = new XLogStreamer(xf, sw, getTestCaseDir(), "oozie.log", 1);
        str.streamLog(new Date(currTime - 10000), new Date(currTime - 5000));
        String[] out = sw.toString().split("\n");
        // Check if the retrieved log content is of length seven lines after filtering based on time window, file name
        // pattern and parameters like JobId, Username etc. and/or based on log level like INFO, DEBUG, etc.
        assertEquals(7, out.length);
        // Check if the lines of the log contain the expected strings
        assertEquals(true, out[0].contains("_L10_"));
        assertEquals(true, out[1].contains("_L11_"));
        assertEquals(true, out[2].contains("_L8_"));
        assertEquals(true, out[3].contains("_L9_"));
        assertEquals(true, out[4].contains("_L1_"));
        assertEquals(true, out[5].contains("_L2_"));
        assertEquals(true, out[6].contains("_L4_"));

        // Test to check if the null values for startTime and endTime are translated to 0 and current time respectively
        // and corresponding log content is retrieved properly
        StringWriter sw1 = new StringWriter();
        XLogStreamer str1 = new XLogStreamer(xf, sw1, getTestCaseDir(), "oozie.log", 1);
        str1.streamLog(null, null);
        out = sw1.toString().split("\n");
        // Check if the retrieved log content is of length eight lines after filtering based on time window, file name
        // pattern and parameters like JobId, Username etc. and/or based on log level like INFO, DEBUG, etc.
        assertEquals(8, out.length);
        // Check if the lines of the log contain the expected strings
        assertEquals(true, out[0].contains("_L10"));
        assertEquals(true, out[1].contains("_L11_"));
        assertEquals(true, out[2].contains("_L8_"));
        assertEquals(true, out[3].contains("_L9_"));
        assertEquals(true, out[4].contains("_L1_"));
        assertEquals(true, out[5].contains("_L2_"));
        assertEquals(true, out[6].contains("_L4_"));
        assertEquals(true, out[7].contains("_L7_"));
    }
}
