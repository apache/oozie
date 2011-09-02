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
package org.apache.oozie.client.rest;

import junit.framework.TestCase;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.rest.JsonCoordinatorAction;
import org.apache.oozie.client.rest.JsonUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class TestJsonCoordinatorAction extends TestCase {


    static String START_TIME = "Fri, 04 Sep 2009 00:00:00 GMT";
    static String END_TIME = "Sat, 05 Sep 2009 00:00:00 GMT";
    static String CREATE_TIME = "Sat, 05 Sep 2009 00:00:00 GMT";
    static String LAST_MODIFIED_TIME = "Sat, 05 Sep 2009 00:00:00 GMT";
    //static List<String> missingDependencies = Arrays.asList("a:a", "a/a", "a//a");
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
        //app.setStartTime(JsonUtils.parseDateRfc822(START_TIME));
        //app.setEndTime(JsonUtils.parseDateRfc822(END_TIME));
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
        //assertEquals(JsonUtils.parseDateRfc822(START_TIME), app.getStartTime());
        //assertEquals(JsonUtils.parseDateRfc822(END_TIME), app.getEndTime());
        assertEquals("http://consoleurl:8080", app.getConsoleUrl());
        assertEquals(missingDependencies, app.getMissingDependencies());
        //assertEquals(3, app.getMissingDependencies().size());

    }

    public void testJsonAndBack() throws Exception {
        JsonCoordinatorAction app = createAppAction();
        StringWriter sw = new StringWriter();
        app.toJSONObject().writeJSONString(sw);
        sw.close();
        JSONObject json = (JSONObject) JSONValue.parse(new StringReader(sw.toString()));
        app = new JsonCoordinatorAction(json);

        assertEquals("a", app.getJobId());
        assertEquals("c", app.getId());
        assertEquals(1, app.getActionNumber());
        assertEquals("cc", app.getRunConf());
        assertEquals("cc", app.getCreatedConf());
        assertEquals("c_e", app.getExternalId());
        assertEquals(JsonUtils.parseDateRfc822(CREATE_TIME), app.getCreatedTime());
        assertEquals(JsonUtils.parseDateRfc822(LAST_MODIFIED_TIME), app.getLastModifiedTime());
        assertEquals(CoordinatorAction.Status.WAITING, app.getStatus());
        // assertEquals(JsonUtils.parseDateRfc822(START_TIME), app.getStartTime());
        //assertEquals(JsonUtils.parseDateRfc822(END_TIME), app.getEndTime());
        assertEquals("http://consoleurl:8080", app.getConsoleUrl());
        assertEquals(missingDependencies, app.getMissingDependencies());
        //assertEquals(3, app.getMissingDependencies().size());

        sw = new StringWriter();
        app.toJSONObject().writeJSONString(sw);
        sw.close();
        json = (JSONObject) JSONValue.parse(new StringReader(sw.toString()));
        app = new JsonCoordinatorAction(json);

        assertEquals("a", app.getJobId());
        assertEquals("c", app.getId());
        assertEquals(1, app.getActionNumber());
        assertEquals("cc", app.getRunConf());
        assertEquals("cc", app.getCreatedConf());
        assertEquals("c_e", app.getExternalId());
        assertEquals(JsonUtils.parseDateRfc822(CREATE_TIME), app.getCreatedTime());
        assertEquals(JsonUtils.parseDateRfc822(LAST_MODIFIED_TIME), app.getLastModifiedTime());
        assertEquals(CoordinatorAction.Status.WAITING, app.getStatus());
        //assertEquals(JsonUtils.parseDateRfc822(START_TIME), app.getStartTime());
        //assertEquals(JsonUtils.parseDateRfc822(END_TIME), app.getEndTime());
        assertEquals("http://consoleurl:8080", app.getConsoleUrl());
        assertEquals(missingDependencies, app.getMissingDependencies());
        //assertEquals(3, app.getMissingDependencies().size());
    }

    public void testList() throws Exception {
        List<JsonCoordinatorAction> actions = Arrays.asList(createAppAction(), createAppAction());
        JSONArray array = JsonCoordinatorAction.toJSONArray(actions);
        StringWriter sw = new StringWriter();
        array.writeJSONString(sw);
        sw.close();
        array = (JSONArray) JSONValue.parse(new StringReader(sw.toString()));
        List<JsonCoordinatorAction> readActions = JsonCoordinatorAction.fromJSONArray(array);
        assertEquals(2, readActions.size());
    }
}
