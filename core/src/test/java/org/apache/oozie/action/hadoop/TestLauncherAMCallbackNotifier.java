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

package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.oozie.QueryServlet;
import org.apache.oozie.action.hadoop.LauncherAM.OozieActionResult;
import org.apache.oozie.command.wf.HangServlet;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.XTestCase;
import org.junit.Assert;
import org.mockito.Mockito;

import java.net.Proxy;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.Servlet;

// A lot of this adapted from org.apache.hadoop.mapreduce.v2.app.TestJobEndNotifier and org.apache.hadoop.mapred.TestJobEndNotifier
public class TestLauncherAMCallbackNotifier extends XTestCase {
    private EmbeddedServletContainer container;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        QueryServlet.lastQueryString = null;
    }

    @Override
    public void tearDown() throws Exception {
        if (container != null) {
            container.stop();
        }

        super.tearDown();
    }

    public void testConfiguration() throws Exception {
        Configuration conf = new Configuration(false);

        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_MAX_ATTEMPTS, "0");
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_ATTEMPTS, "10");
        LauncherAMCallbackNotifier cn = new LauncherAMCallbackNotifier(conf);
        assertEquals(0, cn.numTries);
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_MAX_ATTEMPTS, "1");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals(1, cn.numTries);
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_MAX_ATTEMPTS, "20");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals(11, cn.numTries);  //11 because number of _retries_ is 10

        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL, "1000");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals(1000, cn.waitInterval);
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL, "10000");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals(5000, cn.waitInterval);
        //Test negative numbers are set to default
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL, "-10");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals(5000, cn.waitInterval);

        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_TIMEOUT, "1000");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals(1000, cn.timeout);

        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_PROXY, "somehost");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals(Proxy.Type.DIRECT, cn.proxyToUse.type());
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_PROXY, "somehost:someport");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals(Proxy.Type.DIRECT, cn.proxyToUse.type());
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_PROXY, "somehost:1000");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals("HTTP @ somehost:1000", cn.proxyToUse.toString());
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_PROXY, "socks@somehost:1000");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals("SOCKS @ somehost:1000", cn.proxyToUse.toString());
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_PROXY, "SOCKS@somehost:1000");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals("SOCKS @ somehost:1000", cn.proxyToUse.toString());
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_PROXY, "sfafn@somehost:1000");
        cn = new LauncherAMCallbackNotifier(conf);
        assertEquals("HTTP @ somehost:1000", cn.proxyToUse.toString());
    }

    public void testNotifyRetries() throws InterruptedException {
        Configuration conf = new Configuration(false);
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_ATTEMPTS, "0");
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_MAX_ATTEMPTS, "1");
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_URL, "http://nonexistent");
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL, "5000");

        LauncherAMCallbackNotifier cnSpy = Mockito.spy(new LauncherAMCallbackNotifier(conf));

        long start = System.currentTimeMillis();
        cnSpy.notifyURL(OozieActionResult.SUCCEEDED);
        long end = System.currentTimeMillis();
        Mockito.verify(cnSpy, Mockito.times(1)).notifyURLOnce();
        Assert.assertTrue("Should have taken more than 5 seconds but it only took " + (end - start), end - start >= 5000);

        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_MAX_ATTEMPTS, "3");
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_ATTEMPTS, "3");
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL, "3000");

        cnSpy = Mockito.spy(new LauncherAMCallbackNotifier(conf));
        start = System.currentTimeMillis();
        cnSpy.notifyURL(OozieActionResult.SUCCEEDED);
        end = System.currentTimeMillis();
        Mockito.verify(cnSpy, Mockito.times(3)).notifyURLOnce();
        Assert.assertTrue("Should have taken more than 9 seconds but it only took " + (end - start), end - start >= 9000);
    }

    public void testNotifyTimeout() throws Exception {
        Map<String, String> params = new HashMap<String, String>();
        params.put(HangServlet.SLEEP_TIME_MS, "1000000");
        Configuration conf = setupEmbeddedContainer(HangServlet.class, "/hang/*", "/hang/*", params);

        LauncherAMCallbackNotifier cnSpy = Mockito.spy(new LauncherAMCallbackNotifier(conf));
        long start = System.currentTimeMillis();
        cnSpy.notifyURL(OozieActionResult.SUCCEEDED);
        long end = System.currentTimeMillis();
        Mockito.verify(cnSpy, Mockito.times(1)).notifyURLOnce();
        Assert.assertTrue("Should have taken more than 5 seconds but it only took " + (end - start), end - start >= 5000);
    }

    public void testNotify() throws Exception {
        Configuration conf = setupEmbeddedContainer(QueryServlet.class, "/count/*", "/count/?status=$jobStatus", null);

        LauncherAMCallbackNotifier cn = new LauncherAMCallbackNotifier(conf);

        assertNull(QueryServlet.lastQueryString);
        cn.notifyURL(OozieActionResult.SUCCEEDED);
        waitForCallbackAndCheckResult(FinalApplicationStatus.SUCCEEDED.toString());
    }

    public void testNotifyBackgroundActionWhenSubmitSucceeds() throws Exception {
        Configuration conf = setupEmbeddedContainer(QueryServlet.class, "/count/*", "/count/?status=$jobStatus", null);

        LauncherAMCallbackNotifier cn = new LauncherAMCallbackNotifier(conf);

        assertNull(QueryServlet.lastQueryString);
        cn.notifyURL(OozieActionResult.RUNNING);
        waitForCallbackAndCheckResult(OozieActionResult.RUNNING.toString());
    }

    public void testNotifyBackgroundActionWhenSubmitFailsWithFailed() throws Exception {
        Configuration conf = setupEmbeddedContainer(QueryServlet.class, "/count/*", "/count/?status=$jobStatus", null);

        LauncherAMCallbackNotifier cn = new LauncherAMCallbackNotifier(conf);

        assertNull(QueryServlet.lastQueryString);
        cn.notifyURL(OozieActionResult.FAILED);
        waitForCallbackAndCheckResult(FinalApplicationStatus.FAILED.toString());
    }

    private Configuration setupEmbeddedContainer(Class<? extends Servlet> servletClass, String servletEndPoint,
            String servletUrl, Map<String, String> params) throws Exception {
        container = new EmbeddedServletContainer("test");
        if (servletEndPoint != null) {
            if (params != null) {
                container.addServletEndpoint(servletEndPoint, servletClass, params);
            } else {
                container.addServletEndpoint(servletEndPoint, servletClass);
            }
        }
        container.start();

        Configuration conf = new Configuration(false);
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_ATTEMPTS, "0");
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_MAX_ATTEMPTS, "1");
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_URL, container.getServletURL(servletUrl));
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL, "5000");

        return conf;
    }

    private void waitForCallbackAndCheckResult(final String expectedResult) {
        waitFor(5000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return ("status=" + expectedResult).equals(QueryServlet.lastQueryString);
            }
        });

        assertEquals("status="  + expectedResult, QueryServlet.lastQueryString);
    }
}
