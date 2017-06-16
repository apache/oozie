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

package org.apache.oozie.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestEmbeddedServletContainer extends XTestCase {

    public void testEmbeddedServletContainer() throws Exception {
        EmbeddedServletContainer container = new EmbeddedServletContainer("blah");
        container.addServletEndpoint("/ping/*", PingServlet.class);
        container.addFilter("/ping/*", PingServlet.class);

        try {
            PingServlet.FILTER_INIT = false;
            PingServlet.DO_FILTER = false;
            container.start();
            assertHttpResponse(container);
        }
        finally {
            container.stop();
        }
    }

    public void testMultipleParallelStarts() throws Exception {
        final List<EmbeddedServletContainer> containers = new ArrayList<>();
        PingServlet.FILTER_INIT = false;
        PingServlet.DO_FILTER = false;

        for(int i = 0; i < 10; ++i){
            final EmbeddedServletContainer c = new EmbeddedServletContainer("path"+i);
            c.addServletEndpoint("/ping/*", PingServlet.class);
            c.addFilter("/ping/*", PingServlet.class);
            containers.add(c);
        }

        startAllContainers(containers);

        for(final EmbeddedServletContainer c : containers){
            assertHttpResponse(c);
        }

        stopAllContainers(containers);

    }

    private void stopAllContainers(List<EmbeddedServletContainer> containers) {
        for(final EmbeddedServletContainer c : containers){
            c.stop();
        }
    }

    private void startAllContainers(List<EmbeddedServletContainer> containers) throws Exception {
        for(final EmbeddedServletContainer c : containers){
            c.start();
        }
    }

    private void assertHttpResponse(EmbeddedServletContainer c) throws IOException {
        final URL url = new URL(c.getServletURL("/ping/*") + "bla");
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.connect();
        assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        assertEquals("ping", reader.readLine());
        assertEquals(null, reader.readLine());
        assertTrue(PingServlet.FILTER_INIT);
        assertTrue(PingServlet.DO_FILTER);
    }

}
