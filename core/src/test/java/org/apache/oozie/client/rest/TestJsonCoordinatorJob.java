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

package org.apache.oozie.client.rest;

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;

import junit.framework.TestCase;

public class TestJsonCoordinatorJob extends TestCase {

    static String LAST_ACTION_TIME = "Wed, 02 Sep 2009 00:00:00 GMT";
    static String NEXT_MATERIALIZED_TIME = "Thu, 03 Sep 2009 00:00:00 GMT";
    static String START_TIME = "Fri, 04 Sep 2009 00:00:00 GMT";
    static String END_TIME = "Sat, 05 Sep 2009 00:00:00 GMT";


    static CoordinatorJobBean createApplication() {
        CoordinatorJobBean app = new CoordinatorJobBean();
        app.setAppPath("a");
        app.setAppName("b");
        app.setId("c");
        app.setConf("cc");
        app.setStatus(CoordinatorJob.Status.PREP);
        app.setFrequency("100");
        app.setTimeUnit(CoordinatorJob.Timeunit.WEEK);
        app.setTimeZone("timeZone");
        app.setConcurrency(10);
        app.setExecutionOrder(CoordinatorJob.Execution.FIFO);
        app.setTimeout(100);
        app.setLastActionTime(JsonUtils.parseDateRfc822(LAST_ACTION_TIME));
        app.setNextMaterializedTime(JsonUtils.parseDateRfc822(NEXT_MATERIALIZED_TIME));
        app.setStartTime(JsonUtils.parseDateRfc822(START_TIME));
        app.setEndTime(JsonUtils.parseDateRfc822(END_TIME));
        app.setUser("d");
        app.setGroup("e");
        app.setConsoleUrl("cu");
        return app;
    }

    public void testProperties() {
        CoordinatorJobBean app = createApplication();
        assertEquals("a", app.getAppPath());
        assertEquals("b", app.getAppName());
        assertEquals("c", app.getId());
        assertEquals("cc", app.getConf());
        assertEquals(CoordinatorJob.Status.PREP, app.getStatus());
        assertEquals("100", app.getFrequency());
        assertEquals(CoordinatorJob.Timeunit.WEEK, app.getTimeUnit());
        assertEquals("timeZone", app.getTimeZone());
        assertEquals(10, app.getConcurrency());
        assertEquals(CoordinatorJob.Execution.FIFO, app.getExecutionOrder());
        assertEquals(100, app.getTimeout());
        assertEquals(JsonUtils.parseDateRfc822(LAST_ACTION_TIME), app.getLastActionTime());
        assertEquals(JsonUtils.parseDateRfc822(NEXT_MATERIALIZED_TIME), app.getNextMaterializedTime());
        assertEquals(JsonUtils.parseDateRfc822(START_TIME), app.getStartTime());
        assertEquals(JsonUtils.parseDateRfc822(END_TIME), app.getEndTime());
        assertEquals("d", app.getUser());
        assertEquals("e", app.getGroup());
        assertEquals("cu", app.getConsoleUrl());

    }

}
