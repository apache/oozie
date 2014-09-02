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

package org.apache.oozie;

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.BundleJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.servlet.DagServletTestCase;
import org.apache.oozie.servlet.MockDagEngineService;
import org.apache.oozie.servlet.V1JobsServlet;
import org.apache.oozie.test.XDataTestCase;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Ignore;

public class TestV1JobsServletBundleEngine extends DagServletTestCase {
    static {
        new V1JobsServlet();
    }

    private static final boolean IS_SECURITY_ENABLED = false;

    private Services services;

    /**
     * This class is needed in order to reuse some methods of class {@link XDataTestCase}. We cannot directly extend it there as
     * we extend {@link DagServletTestCase}. Anonymous inner class is also not an option since we cannot assign it an annotation.
     * The @Ignore annotation is needed to prevent JUnit from recognizing this inner class as a test.
     */
    @Ignore
    private static class XDataTestCase1 extends XDataTestCase {
    }

    private final XDataTestCase1 xDataTestCase = new XDataTestCase1();

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

    /**
     * Tests method {@link BundleEngine#getBundleJobs(String, int, int)}. Also
     * tests positive cases of the filter parsing by
     * {@link BundleEngine#parseFilter(String)}.
     */
    public void testGetBundleJobs() throws Exception {
        final BundleJobBean bundleJobBean = xDataTestCase.addRecordToBundleJobTable(Job.Status.PREP, false);

        runTest("/v1/jobs", V1JobsServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                MockDagEngineService.reset();

                Map<String, String> params = new HashMap<String, String>();
                params.put(RestConstants.JOBTYPE_PARAM, "bundle");
                params.put(RestConstants.JOBS_FILTER_PARAM, OozieClient.FILTER_STATUS + "=PREP;" + OozieClient.FILTER_NAME
                        + "=BUNDLE-TEST;" + OozieClient.FILTER_USER + "=" + getTestUser());

                URL url = createURL("", params);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
                assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));

                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));

                assertEquals(Long.valueOf(1L), json.get("total"));
                JSONArray array = (JSONArray) json.get("bundlejobs");
                JSONObject jo = (JSONObject) array.get(0);
                assertEquals(bundleJobBean.getId(), jo.get("bundleJobId"));

                return null;
            }
        });
    }

}
