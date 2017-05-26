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

package org.apache.oozie.command.wf;

import org.apache.oozie.util.XLog;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Servlet that 'hangs' for some amount of time (200ms) by default.
 * The time can be configured by setting {@link HangServlet#SLEEP_TIME_MS} as an init parameter for the servlet.
 */
public class HangServlet extends HttpServlet {

    public static final String SLEEP_TIME_MS = "sleep_time_ms";

    protected void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
        try {
            long time = 200;
            String sleeptime = getInitParameter(SLEEP_TIME_MS);
            if (sleeptime != null) {
                try {
                    time = Long.parseLong(sleeptime);
                } catch (NumberFormatException nfe) {
                    XLog.getLog(HangServlet.class).error("Invalid sleep time, using default (200)", nfe);
                }
            }
            XLog.getLog(HangServlet.class).info("Sleeping for {0} ms", time);
            Thread.sleep(time);
        }
        catch (Exception ex) {
            //NOP
        }
    }

}
