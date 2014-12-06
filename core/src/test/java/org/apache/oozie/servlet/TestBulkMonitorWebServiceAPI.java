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

package org.apache.oozie.servlet;

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.executor.jpa.BundleJobInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordActionInsertJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobsGetFromParentIdJPAExecutor;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.AuthorizationService;
import org.apache.oozie.service.BundleEngineService;
import org.apache.oozie.service.ForTestAuthorizationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.ProxyUserService;
import org.apache.oozie.service.Services;
import org.apache.oozie.servlet.AuthFilter;
import org.apache.oozie.servlet.HostnameFilter;
import org.apache.oozie.servlet.V1JobsServlet;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class TestBulkMonitorWebServiceAPI extends XDataTestCase {
    private EmbeddedServletContainer container;
    private String servletPath;
    Services services;
    JPAService jpaService;

    static {
        new V1JobsServlet();
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        LocalOozie.start();
        jpaService = Services.get().get(JPAService.class);
        addRecordsForBulkMonitor();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    private URL createURL(String servletPath, String resource, Map<String, String> parameters) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(container.getServletURL(servletPath));
        if (resource != null && resource.length() > 0) {
            sb.append("/").append(resource);
        }
        if (parameters.size() > 0) {
            String separator = "?";
            for (Map.Entry<String, String> param : parameters.entrySet()) {
                sb.append(separator).append(URLEncoder.encode(param.getKey(), "UTF-8")).append("=")
                        .append(URLEncoder.encode(param.getValue(), "UTF-8"));
                separator = "&";
            }
        }
        return new URL(sb.toString());
    }

    private URL createURL(String resource, Map<String, String> parameters) throws Exception {
        return createURL(servletPath, resource, parameters);
    }

    private void runTest(String servletPath, Class servletClass, boolean securityEnabled, Callable<Void> assertions)
            throws Exception {
        runTest(new String[] { servletPath }, new Class[] { servletClass }, securityEnabled, assertions);
    }

    private void runTest(String[] servletPath, Class[] servletClass, boolean securityEnabled, Callable<Void> assertions)
            throws Exception {
        Services services = new Services();
        this.servletPath = servletPath[0];
        try {
            String proxyUser = getTestUser();
            services.getConf().set(ProxyUserService.CONF_PREFIX + proxyUser + ProxyUserService.HOSTS, "*");
            services.getConf().set(ProxyUserService.CONF_PREFIX + proxyUser + ProxyUserService.GROUPS, "*");
            services.init();
            services.getConf().setBoolean(AuthorizationService.CONF_SECURITY_ENABLED, securityEnabled);
            Services.get().setService(ForTestAuthorizationService.class);
            Services.get().setService(BundleEngineService.class);
            container = new EmbeddedServletContainer("oozie");
            for (int i = 0; i < servletPath.length; i++) {
                container.addServletEndpoint(servletPath[i], servletClass[i]);
            }
            container.addFilter("*", HostnameFilter.class);
            container.addFilter("*", AuthFilter.class);
            setSystemProperty("user.name", getTestUser());
            container.start();
            assertions.call();
        }
        finally {
            this.servletPath = null;
            if (container != null) {
                container.stop();
            }
            services.destroy();
            container = null;
        }
    }

    public void testSingleRecord() throws Exception {
        runTest("/v1/jobs", V1JobsServlet.class, false, new Callable<Void>() {
            public Void call() throws Exception {

                String bulkRequest = "bundle=" + bundleName + ";coordinators=Coord1;"
                        + "actionStatus=FAILED;startcreatedtime=2012-07-21T00:00Z";
                JSONArray array = _requestToServer(bulkRequest);

                assertEquals(1, array.size());
                JSONObject jbundle = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_BUNDLE);
                JSONObject jcoord = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_COORDINATOR);
                JSONObject jaction = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_ACTION);

                assertEquals(jbundle.get(JsonTags.BUNDLE_JOB_NAME), "BUNDLE-TEST");
                assertEquals(jcoord.get(JsonTags.COORDINATOR_JOB_NAME), "Coord1");
                assertEquals(jcoord.get(JsonTags.COORDINATOR_JOB_STATUS), "RUNNING");
                assertEquals(jaction.get(JsonTags.COORDINATOR_ACTION_STATUS), "FAILED");
                assertEquals((jaction.get(JsonTags.COORDINATOR_ACTION_CREATED_TIME).toString().split(", "))[1],
                        DateUtils.parseDateUTC(CREATE_TIME).toGMTString());
                return null;
            }
        });
    }

    public void testMultipleRecords() throws Exception {
        runTest("/v1/jobs", V1JobsServlet.class, false, new Callable<Void>() {
            public Void call() throws Exception {

                String bulkRequest = "bundle=" + bundleName + ";coordinators=Coord1;"
                        + "actionStatus=FAILED,KILLED;startcreatedtime=2012-07-21T00:00Z";
                JSONArray array = _requestToServer(bulkRequest);

                assertEquals(2, array.size());
                JSONObject jbundle = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_BUNDLE);
                assertNotNull(jbundle);
                JSONObject jaction1 = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_ACTION);
                JSONObject jaction2 = (JSONObject) ((JSONObject) array.get(1)).get(JsonTags.BULK_RESPONSE_ACTION);

                assertNotNull(jaction1);
                assertNotNull(jaction2);

                assertEquals(jbundle.get(JsonTags.BUNDLE_JOB_NAME), bundleName);
                assertEquals(jaction1.get(JsonTags.COORDINATOR_ACTION_STATUS), "FAILED");
                assertEquals(jaction2.get(JsonTags.COORDINATOR_ACTION_STATUS), "KILLED");

                return null;
            }
        });
    }

    public void testNoRecords() throws Exception {
        runTest("/v1/jobs", V1JobsServlet.class, false, new Callable<Void>() {
            public Void call() throws Exception {
                String bulkRequest = "bundle=BUNDLE-ABC";
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOBS_BULK_PARAM, bulkRequest);
                params.put(RestConstants.OFFSET_PARAM, "1");
                params.put(RestConstants.LEN_PARAM, "5");
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                // WS call will throw BAD_REQUEST code 400 error because no
                // records found for this bundle
                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());

                return null;
            }
        });
    }

    public void testMultipleCoordinators() throws Exception {
        runTest("/v1/jobs", V1JobsServlet.class, false, new Callable<Void>() {
            public Void call() throws Exception {
                // giving range as 2 of the total 3 coordinators
                String bulkRequest = "bundle=" + bundleName + ";coordinators=Coord1,Coord2;actionstatus=KILLED";
                JSONArray array = _requestToServer(bulkRequest);

                assertEquals(2, array.size());
                JSONObject jbundle = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_BUNDLE);
                assertNotNull(jbundle);
                JSONObject jaction1 = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_ACTION);
                JSONObject jaction2 = (JSONObject) ((JSONObject) array.get(1)).get(JsonTags.BULK_RESPONSE_ACTION);

                assertNotNull(jaction1);
                assertNotNull(jaction2);

                assertEquals(jaction1.get(JsonTags.COORDINATOR_ACTION_ID), "Coord1@2");
                assertEquals(jaction2.get(JsonTags.COORDINATOR_ACTION_ID), "Coord2@1");

                return null;
            }
        });
    }

    public void testDefaultStatus() throws Exception {

        // adding coordinator action #4 to Coord#3
        CoordinatorActionBean action4 = new CoordinatorActionBean();
        action4.setId("Coord3@1");
        action4.setStatus(CoordinatorAction.Status.FAILED);
        action4.setCreatedTime(DateUtils.parseDateUTC(CREATE_TIME));
        action4.setJobId("Coord3");

        Calendar cal = Calendar.getInstance();
        cal.setTime(DateUtils.parseDateUTC(CREATE_TIME));
        cal.add(Calendar.DATE, -1);

        action4.setNominalTime(cal.getTime());
        CoordActionInsertJPAExecutor actionInsert = new CoordActionInsertJPAExecutor(action4);
        jpaService.execute(actionInsert);

        runTest("/v1/jobs", V1JobsServlet.class, false, new Callable<Void>() {
            public Void call() throws Exception {
                String bulkRequest = "bundle=" + bundleName;
                JSONArray array = _requestToServer(bulkRequest);

                assertEquals(4, array.size());

                return null;
            }
        });
    }

    public void testMultipleBundleIdsForName() throws Exception {

        // Adding another bundle having same name
        BundleJobBean bundle = new BundleJobBean();
        bundle.setId("00002-12345-B");
        bundle.setAppName(bundleName);
        bundle.setStatus(BundleJob.Status.RUNNING);
        bundle.setStartTime(new Date());
        BundleJobInsertJPAExecutor bundleInsert = new BundleJobInsertJPAExecutor(bundle);
        jpaService.execute(bundleInsert);

        runTest("/v1/jobs", V1JobsServlet.class, false, new Callable<Void>() {
            public Void call() throws Exception {
                String bulkRequest = "bundle=" + bundleName;
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOBS_BULK_PARAM, bulkRequest);
                params.put(RestConstants.OFFSET_PARAM, "1");
                params.put(RestConstants.LEN_PARAM, "5");
                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                // WS call will throw BAD_REQUEST code 400 error because no
                // records found for this bundle
                assertFalse(HttpServletResponse.SC_BAD_REQUEST == conn.getResponseCode());

                return null;
            }
        });
    }

    public void testBundleId() throws Exception {
        runTest("/v1/jobs", V1JobsServlet.class, false, new Callable<Void>() {
            public Void call() throws Exception {

                String bulkRequest = "bundle=" + bundleId + ";coordinators=Coord1;"
                        + "actionStatus=FAILED;startcreatedtime=2012-07-21T00:00Z";
                JSONArray array = _requestToServer(bulkRequest);

                assertEquals(1, array.size());
                JSONObject jbundle = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_BUNDLE);
                JSONObject jcoord = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_COORDINATOR);
                JSONObject jaction = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_ACTION);

                assertNotNull(jbundle);
                assertNotNull(jcoord);
                assertNotNull(jaction);

                assertEquals(jbundle.get(JsonTags.BUNDLE_JOB_ID), bundleId);
                assertEquals(jcoord.get(JsonTags.COORDINATOR_JOB_NAME), "Coord1");
                assertEquals(jcoord.get(JsonTags.COORDINATOR_JOB_STATUS), "RUNNING");
                assertEquals(jaction.get(JsonTags.COORDINATOR_ACTION_STATUS), "FAILED");
                assertEquals((jaction.get(JsonTags.COORDINATOR_ACTION_CREATED_TIME).toString().split(", "))[1],
                        DateUtils.parseDateUTC(CREATE_TIME).toGMTString());
                return null;
            }
        });
    }

    public void testBundleIdWithCoordId() throws Exception {
        // fetching coord Ids
        JPAService jpaService = Services.get().get(JPAService.class);
        List<String> coordIds = jpaService.execute(new CoordJobsGetFromParentIdJPAExecutor(bundleId, 10));
        // there are 3 coordinators but giving range as only two of them
        final String coordIdsStr = coordIds.get(0) + "," + coordIds.get(1);

        runTest("/v1/jobs", V1JobsServlet.class, false, new Callable<Void>() {
            public Void call() throws Exception {
                // giving range as 2 of the total 3 coordinators
                String bulkRequest = "bundle=" + bundleId + ";coordinators=" + coordIdsStr + ";actionstatus=KILLED";
                JSONArray array = _requestToServer(bulkRequest);

                assertEquals(2, array.size());
                JSONObject jbundle = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_BUNDLE);
                JSONObject jaction1 = (JSONObject) ((JSONObject) array.get(0)).get(JsonTags.BULK_RESPONSE_ACTION);
                JSONObject jaction2 = (JSONObject) ((JSONObject) array.get(1)).get(JsonTags.BULK_RESPONSE_ACTION);

                assertEquals(jaction1.get(JsonTags.COORDINATOR_ACTION_ID), "Coord1@2");
                assertEquals(jaction2.get(JsonTags.COORDINATOR_ACTION_ID), "Coord2@1");

                return null;
            }
        });
    }

    private JSONArray _requestToServer(String bulkRequest) throws Exception {
        Map<String, String> params = new HashMap<String, String>();
        params.put(RestConstants.JOBS_BULK_PARAM, bulkRequest);
        params.put(RestConstants.OFFSET_PARAM, "1");
        params.put(RestConstants.LEN_PARAM, "5");
        URL url = createURL("", params);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
        assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
        JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
        JSONArray array = (JSONArray) json.get(JsonTags.BULK_RESPONSES);
        return array;
    }

}
