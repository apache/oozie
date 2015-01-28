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


package org.apache.oozie.service;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Date;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.servlet.HostnameFilter;
import org.apache.oozie.servlet.V2AdminServlet;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.ZKXTestCase;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class TestHAShareLibService extends ZKXTestCase {

    EmbeddedServletContainer container;

    FileSystem fs;

    static {
        new V2AdminServlet();

    }

    protected void setUp() throws Exception {
        super.setUp();
        container = new EmbeddedServletContainer("oozie");
        container.addServletEndpoint("/v2/admin/*", V2AdminServlet.class);
        container.addServletEndpoint("/other-oozie-server/*", DummyV2AdminServlet.class);
        container.addFilter("*", HostnameFilter.class);
        container.start();
        Services.get().setService(ShareLibService.class);
        Services.get().getConf().setBoolean(AuthorizationService.CONF_SECURITY_ENABLED, false);

        Services.get().setService(ZKJobsConcurrencyService.class);

        Path launcherlibPath = Services.get().get(WorkflowAppService.class).getSystemLibPath();
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        URI uri = launcherlibPath.toUri();
        fs = FileSystem.get(has.createJobConf(uri.getAuthority()));
        Date time = new Date(System.currentTimeMillis());

        Path basePath = new Path(Services.get().getConf().get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path libpath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + ShareLibService.dateFormat.format(time));
        fs.mkdirs(libpath);

        Path pigPath = new Path(libpath.toString() + Path.SEPARATOR + "pig");
        Path pigPath1 = new Path(libpath.toString() + Path.SEPARATOR + "pig_9");
        Path pigPath2 = new Path(libpath.toString() + Path.SEPARATOR + "pig_10");
        fs.mkdirs(pigPath);
        fs.mkdirs(pigPath1);
        fs.mkdirs(pigPath2);
        fs.create(new Path(libpath.toString() + Path.SEPARATOR + "pig_10" + Path.SEPARATOR + "pig-10.jar")).close();

    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testShareLibWithHA() throws Exception {
        ZKJobsConcurrencyService zkjcs = new ZKJobsConcurrencyService();
        zkjcs.init(Services.get());
        DummyZKOozie dummyOozie_1 = null;
        DummyZKOozie dummyOozie_2 = null;
        try {
            dummyOozie_1 = new DummyZKOozie("9876", container.getServletURL("/other-oozie-server/*"));
            String url = container.getServletURL("/v2/admin/*") + "update_sharelib?" + RestConstants.ALL_SERVER_REQUEST
                    + "=true";
            HttpClient client = new HttpClient();
            GetMethod method = new GetMethod(url);
            int statusCode = client.executeMethod(method);
            assertEquals(HttpURLConnection.HTTP_OK, statusCode);
            Reader reader = new InputStreamReader(method.getResponseBodyAsStream());
            JSONArray sharelib = (JSONArray) JSONValue.parse(reader);
            assertEquals(2, sharelib.size());
            // 1st server update is successful
            JSONObject obj = (JSONObject) sharelib.get(0);
            assertEquals("Successful",
                    ((JSONObject) obj.get(JsonTags.SHARELIB_LIB_UPDATE)).get(JsonTags.SHARELIB_UPDATE_STATUS));

            // 2nd server update is successful.
            obj = (JSONObject) sharelib.get(1);
            assertEquals("Successful",
                    ((JSONObject) obj.get(JsonTags.SHARELIB_LIB_UPDATE)).get(JsonTags.SHARELIB_UPDATE_STATUS));

            // 3rd server not defined.should throw exception.
            dummyOozie_2 = new DummyZKOozie("9873", container.getServletURL("/") + "not-defined/");

            statusCode = client.executeMethod(method);
            assertEquals(HttpURLConnection.HTTP_OK, statusCode);
            reader = new InputStreamReader(method.getResponseBodyAsStream());
            sharelib = (JSONArray) JSONValue.parse(reader);
            assertEquals(3, sharelib.size());

            obj = (JSONObject) sharelib.get(0);
            String status1 = ((JSONObject) obj.get(JsonTags.SHARELIB_LIB_UPDATE)).get(JsonTags.SHARELIB_UPDATE_STATUS)
                    .toString();

            obj = (JSONObject) sharelib.get(1);
            String status2 = ((JSONObject) obj.get(JsonTags.SHARELIB_LIB_UPDATE)).get(JsonTags.SHARELIB_UPDATE_STATUS)
                    .toString();

            obj = (JSONObject) sharelib.get(2);
            String status3 = ((JSONObject) obj.get(JsonTags.SHARELIB_LIB_UPDATE)).get(JsonTags.SHARELIB_UPDATE_STATUS)
                    .toString();

            int success = 0;
            int notSuccess = 0;

            if (status1.equals("Successful")) {
                success++;
            }
            else {
                notSuccess++;
            }

            if (status2.equals("Successful")) {
                success++;
            }
            else {
                notSuccess++;
            }
            if (status3.equals("Successful")) {
                success++;
            }
            else {
                notSuccess++;
            }
            // 1 fails and other 2 succeed.
            assertEquals(1, notSuccess);
            assertEquals(2, success);
        }
        finally {
            if (dummyOozie_1 != null) {
                dummyOozie_1.teardown();
            }

            if (dummyOozie_2 != null) {
                dummyOozie_2.teardown();
            }
            zkjcs.destroy();
            container.stop();
        }

    }

}
