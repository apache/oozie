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
import org.apache.oozie.command.wf.HangServlet;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.XTestCase;
import org.junit.Assert;
import org.mockito.Mockito;

import java.net.Proxy;
import java.util.HashMap;
import java.util.Map;

// A lot of this adapted from org.apache.hadoop.mapreduce.v2.app.TestJobEndNotifier and org.apache.hadoop.mapred.TestJobEndNotifier
public class TestLauncherAMCallbackNotifier extends XTestCase {

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
        cnSpy.notifyURL(FinalApplicationStatus.SUCCEEDED);
        long end = System.currentTimeMillis();
        Mockito.verify(cnSpy, Mockito.times(1)).notifyURLOnce();
        Assert.assertTrue("Should have taken more than 5 seconds but it only took " + (end - start), end - start >= 5000);

        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_MAX_ATTEMPTS, "3");
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_ATTEMPTS, "3");
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL, "3000");

        cnSpy = Mockito.spy(new LauncherAMCallbackNotifier(conf));
        start = System.currentTimeMillis();
        cnSpy.notifyURL(FinalApplicationStatus.SUCCEEDED);
        end = System.currentTimeMillis();
        Mockito.verify(cnSpy, Mockito.times(3)).notifyURLOnce();
        Assert.assertTrue("Should have taken more than 9 seconds but it only took " + (end - start), end - start >= 9000);
    }

    public void testNotifyTimeout() throws Exception {
        EmbeddedServletContainer container = null;
        try {
            container = new EmbeddedServletContainer("blah");
            Map<String, String> params = new HashMap<String, String>();
            params.put(HangServlet.SLEEP_TIME_MS, "1000000");
            container.addServletEndpoint("/hang/*", HangServlet.class, params);
            container.start();

            Configuration conf = new Configuration(false);
            conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_ATTEMPTS, "0");
            conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_MAX_ATTEMPTS, "1");
            conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_URL, container.getServletURL("/hang/*"));
            conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL, "5000");

            LauncherAMCallbackNotifier cnSpy = Mockito.spy(new LauncherAMCallbackNotifier(conf));
            long start = System.currentTimeMillis();
            cnSpy.notifyURL(FinalApplicationStatus.SUCCEEDED);
            long end = System.currentTimeMillis();
            Mockito.verify(cnSpy, Mockito.times(1)).notifyURLOnce();
            Assert.assertTrue("Should have taken more than 5 seconds but it only took " + (end - start), end - start >= 5000);
        } finally {
            if (container != null) {
                container.stop();
            }
        }
    }

    public void testNotify() throws Exception {
        EmbeddedServletContainer container = null;
        try {
            container = new EmbeddedServletContainer("blah");
            container.addServletEndpoint("/count/*", QueryServlet.class);
            container.start();

            Configuration conf = new Configuration(false);
            conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_ATTEMPTS, "0");
            conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_MAX_ATTEMPTS, "1");
            conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_URL, container.getServletURL("/count/?status=$jobStatus"));
            conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_RETRY_INTERVAL, "5000");

            LauncherAMCallbackNotifier cn = new LauncherAMCallbackNotifier(conf);
            QueryServlet.lastQueryString = null;
            assertNull(QueryServlet.lastQueryString);
            cn.notifyURL(FinalApplicationStatus.SUCCEEDED);
            waitFor(5000, new Predicate() {
                @Override
                public boolean evaluate() throws Exception {
                    return "status=SUCCEEDED".equals(QueryServlet.lastQueryString);
                }
            });
            assertEquals("status=SUCCEEDED", QueryServlet.lastQueryString);
        } finally {
            if (container != null) {
                container.stop();
            }
        }
    }
}
