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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Ignore;

public class TestV1JobServletBundleEngine extends DagServletTestCase {
    static {
        new V1JobServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;

    private Services services;

    /**
     * This class is needed in order to reuse some methods of class {@link XDataTestCase}. We cannot directly extend it there as
     * we extend {@link DagServletTestCase}. Anonymous inner class is also not an option since we cannot assign it an annotation.
     * The @Ignore annotation is needed to prevent JUnit from recognizing this inner class as a test.
     */
    @Ignore
    private static class XDataTestCase1 extends XDataTestCase {}

    private final XDataTestCase xDataTestCase = new XDataTestCase1();

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        xDataTestCase.setName(getName());
        xDataTestCase.setUpPub();

        new Services().init();
        services = Services.get();
        services.setService(UUIDService.class);
    }

    @Override
    protected void tearDown() throws Exception {
        if (services != null) {
            services.destroy();
        }
        xDataTestCase.tearDownPub();
        super.tearDown();
    }

    public void testBundleEngineGetBundleJob() throws Exception {
        final BundleJobBean bundleJobBean = xDataTestCase.addRecordToBundleJobTable(Job.Status.PREP, false);

        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO);
                final String id = bundleJobBean.getId();
                URL url = createURL(id, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());

                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
                JSONObject obj = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                assertEquals(id, obj.get("bundleJobId"));
                return null;
            }
        });
    }

    public void testBundleEngineChange() throws Exception {
        final BundleJobBean bundleJobBean = xDataTestCase.addRecordToBundleJobTable(Job.Status.PREP, false);

        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_CHANGE);
                String changeValue = "endtime=2011-12-01T05:00Z";
                params.put(RestConstants.JOB_CHANGE_VALUE, changeValue);
                final String id = bundleJobBean.getId();
                URL url = createURL(id, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("PUT");

                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                return null;
            }
        });
    }

    public void testBundleEngineGetDefinition() throws Exception {
        final BundleJobBean bundleJobBean = xDataTestCase.addRecordToBundleJobTable(Job.Status.PREP, false);

        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_DEFINITION);
                final String id = bundleJobBean.getId();
                URL url = createURL(id, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());

                String ct = conn.getHeaderField("content-type");
                assertTrue(ct.startsWith(RestConstants.XML_CONTENT_TYPE));
                String response = IOUtils.getReaderAsString(new InputStreamReader(conn.getInputStream()), -1);
                assertTrue(response!= null && response.length() > 0);

                return null;
            }
        });
    }

    public void testBundleEngineStreamLog() throws Exception {
        final BundleJobBean bundleJobBean = xDataTestCase.addRecordToBundleJobTable(Job.Status.PREP, false);

        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_LOG);
                final String id = bundleJobBean.getId();
                URL url = createURL(id, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                return null;
            }
        });
    }

    public void testBundleEngineKill() throws Exception {
        _testBundleEnginePutImpl(RestConstants.JOB_ACTION_KILL);
    }

    public void testBundleEngineResume() throws Exception {
        _testBundleEnginePutImpl(RestConstants.JOB_ACTION_RESUME);
    }

    public void testBundleEngineSuspend() throws Exception {
        _testBundleEnginePutImpl(RestConstants.JOB_ACTION_SUSPEND);
    }

    public void testBundleEngineStart() throws Exception {
        _testBundleEnginePutImpl(RestConstants.JOB_ACTION_START);
    }

    public void testBundleEngineReRun() throws Exception {
        _testBundleEnginePutImpl(RestConstants.JOB_BUNDLE_ACTION_RERUN);
    }

    private void _testBundleEnginePutImpl(final String jobAction) throws Exception {
        final BundleJobBean bundleJobBean = xDataTestCase.addRecordToBundleJobTable(Job.Status.PREP, false);

        runTest("/v1/job/*", V1JobServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockCoordinatorEngineService.reset();
                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.ACTION_PARAM, jobAction);
                final String id = bundleJobBean.getId();
                URL url = createURL(id, params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
                conn.setRequestMethod("PUT");

                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                return null;
            }
        });
    }

}
