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
import java.io.IOException;
import java.io.StringWriter;
import org.apache.oozie.test.XTestCase;

public class TestSimplifiedTimestampedMessageParser extends XTestCase {

    public void testProcessRemainingLog() throws IOException {
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

        File file = TestTimestampedMessageParser.prepareFile1(getTestCaseDir());
        StringWriter sw = new StringWriter();
        new SimpleTimestampedMessageParser(new BufferedReader(new FileReader(file)), xf).processRemaining(sw, 4096);
        String[] out = sw.toString().split("\n");
        assertEquals(19, out.length);
        assertTrue(out[0].contains("_L1_"));
        assertTrue(out[1].contains("_L2_"));
        assertTrue(out[2].contains("_L3_"));
        assertTrue(out[3].contains("_L3A_"));
        assertTrue(out[4].contains("_L3B_"));
        assertTrue(out[5].contains("_L4_"));
        assertTrue(out[6].contains("_L5_"));
        assertTrue(out[7].contains("_L6_"));
        assertTrue(out[8].contains("_L7_"));
        assertTrue(out[9].contains("_L8_"));
        assertTrue(out[10].contains("_L9_"));
        assertTrue(out[11].contains("_L10_"));
        assertTrue(out[12].contains("_L11_"));
        assertTrue(out[13].contains("_L12_"));
        assertTrue(out[14].contains("_L13_"));
        assertTrue(out[15].contains("_L14_"));
        assertTrue(out[16].contains("_L15_"));
        assertTrue(out[17].contains("_L16_"));
        assertTrue(out[18].contains("_L17_"));
    }

    public void testProcessRemainingCoordinatorLogForActions() throws IOException {
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

        File file = TestTimestampedMessageParser.prepareFile2(getTestCaseDir());
        StringWriter sw = new StringWriter();
        new SimpleTimestampedMessageParser(new BufferedReader(new FileReader(file)), xf).processRemaining(sw, 4096);
        String[] matches = sw.toString().split("\n");
        assertEquals(9, matches.length);
        assertTrue(matches[0].contains("_L1_"));
        assertTrue(matches[1].contains("_L2_"));
        assertTrue(matches[2].contains("_L3_"));
        assertTrue(matches[3].contains("_L3A_"));
        assertTrue(matches[4].contains("_L3B_"));
        assertTrue(matches[5].contains("_L4_"));
        assertTrue(matches[6].contains("_L5_"));
        assertTrue(matches[7].contains("_L6_"));
        assertTrue(matches[8].contains("_L7_"));
    }
}
