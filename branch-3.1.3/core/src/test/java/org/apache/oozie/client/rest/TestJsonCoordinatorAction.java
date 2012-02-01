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

import junit.framework.TestCase;

import org.apache.oozie.client.CoordinatorAction;

public class TestJsonCoordinatorAction extends TestCase {

    static String CREATE_TIME = "Sat, 05 Sep 2009 00:00:00 GMT";
    static String LAST_MODIFIED_TIME = "Sat, 05 Sep 2009 00:00:00 GMT";
    static String missingDependencies = "a:a, a/a, a//a";

    static JsonCoordinatorAction createAppAction() {
        JsonCoordinatorAction app = new JsonCoordinatorAction();
        app.setJobId("a");
        app.setId("c");
        app.setActionNumber(1);
        app.setRunConf("cc");
        app.setCreatedConf("cc");
        app.setExternalId("c_e");
        app.setCreatedTime(JsonUtils.parseDateRfc822(CREATE_TIME));
        app.setLastModifiedTime(JsonUtils.parseDateRfc822(LAST_MODIFIED_TIME));
        app.setStatus(CoordinatorAction.Status.WAITING);
        app.setConsoleUrl("http://consoleurl:8080");
        app.setMissingDependencies(missingDependencies);
        return app;
    }

    public void testProperties() {
        JsonCoordinatorAction app = createAppAction();
        assertEquals("a", app.getJobId());
        assertEquals("c", app.getId());
        assertEquals(1, app.getActionNumber());
        assertEquals("cc", app.getRunConf());
        assertEquals("cc", app.getCreatedConf());
        assertEquals("c_e", app.getExternalId());
        assertEquals(JsonUtils.parseDateRfc822(CREATE_TIME), app.getCreatedTime());
        assertEquals(JsonUtils.parseDateRfc822(LAST_MODIFIED_TIME), app.getLastModifiedTime());
        assertEquals(CoordinatorAction.Status.WAITING, app.getStatus());
        assertEquals("http://consoleurl:8080", app.getConsoleUrl());
        assertEquals(missingDependencies, app.getMissingDependencies());

    }

}
