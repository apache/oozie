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

import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLogStreamer;

import java.util.ArrayList;

import org.apache.oozie.test.XTestCase;

public class TestXLogFilter extends XTestCase {
    public void testXLogFileter() throws ServiceException {
        Services services = new Services();
        services.init();
        try {
            XLogStreamer.Filter xf2 = new XLogStreamer.Filter();
            xf2.constructPattern();
            ArrayList<String> a = new ArrayList<String>();
            a.add("2009-06-24 02:43:13,958 DEBUG");
            a.add(" WorkflowRunnerCallable:323 - " + XLog.Info.get().createPrefix() + " test log");
            assertEquals(true, xf2.matches(a));
        }
        finally {
            services.destroy();
        }

        XLogStreamer.Filter.reset();
        XLogStreamer.Filter.defineParameter("USER");
        XLogStreamer.Filter.defineParameter("GROUP");
        XLogStreamer.Filter.defineParameter("TOKEN");
        XLogStreamer.Filter.defineParameter("APP");
        XLogStreamer.Filter.defineParameter("JOB");
        XLogStreamer.Filter.defineParameter("ACTION");
        XLogStreamer.Filter xf = new XLogStreamer.Filter();

        assertEquals(7, matches(xf));
        xf.setLogLevel(XLog.Level.WARN.toString());
        assertEquals(2, matches(xf));

        xf.setLogLevel(XLog.Level.WARN.toString());
        xf.setParameter("APP", "example-forkjoinwf");
        assertEquals(0, matches(xf));

        xf.setLogLevel(XLog.Level.DEBUG.toString() + "|" + XLog.Level.INFO.toString());
        xf.setParameter("JOB", "14-200904160239--example-forkjoinwf");
        assertEquals(2, matches(xf));

        XLogStreamer.Filter xf1 = new XLogStreamer.Filter();
        xf1.setParameter("USER", "oozie");
        assertEquals(3, matches(xf1));

        xf1.setParameter("GROUP", "oozie");
        assertEquals(2, matches(xf1));

        xf1.setParameter("TOKEN", "MYtoken");
        assertEquals(1, matches(xf1));
    }

    private int matches(XLogStreamer.Filter xf) {
        xf.constructPattern();
        ArrayList<String> a = new ArrayList<String>();
        a.add("2009-06-24 02:43:13,958 DEBUG WorkflowRunnerCallable:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-forkjoinwf] JOB[14-200904160239--example-forkjoinwf] ACTION[-] End workflow state change");
        a.add("2009-06-24 02:43:13,961  INFO WorkflowRunnerCallable:317 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] JOB[14-200904160239--example-forkjoinwf] ACTION[-] [org.apache.oozie.core.command.WorkflowRunnerCallable] released lock");
        a.add("2009-06-24 02:43:13,986  WARN JobClient:539 - Use GenericOptionsParser for parsing the arguments. Applications should implement Tool for the same.");
        a.add("2009-06-24 02:43:14,431  WARN JobClient:661 - No job jar file set.  User classes may not be found. See JobConf(Class) or JobConf#setJar(String).");
        a.add("2009-06-24 02:43:14,505  INFO ActionExecutorCallable:317 - USER[oozie] GROUP[oozie] TOKEN[-] APP[-] JOB[-] ACTION[-] Released Lock");
        a.add("2009-06-24 02:43:19,344 DEBUG PendingSignalsCallable:323 - USER[oozie] GROUP[oozie] TOKEN[MYtoken] APP[-] JOB[-] ACTION[-] Number of pending signals to check [0]");
        a.add("2009-06-24 02:43:29,151 DEBUG PendingActionsCallable:323 - USER[-] GROUP[-] TOKEN[-] APP[-] JOB[-] ACTION[-] Number of pending actions [0] ");
        int matchCnt = 0;
        for (int i = 0; i < a.size(); i++) {
            if (xf.matches(xf.splitLogMessage(a.get(i)))) {
                matchCnt++;
            }
        }
        return matchCnt;
    }
}
