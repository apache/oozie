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
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class TestEmbeddedServletContainer extends XTestCase {

    public void testEmbeddedServletContainer() throws Exception {
        EmbeddedServletContainer container = new EmbeddedServletContainer("blah");
        container.addServletEndpoint("/ping/*", PingServlet.class);
        container.addFilter("/ping/*", PingServlet.class);

        try {
            PingServlet.FILTER_INIT = false;
            PingServlet.DO_FILTER = false;
            container.start();
            URL url = new URL(container.getServletURL("/ping/*") + "bla");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.connect();
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            assertEquals("ping", reader.readLine());
            assertEquals(null, reader.readLine());
            assertTrue(PingServlet.FILTER_INIT);
            assertTrue(PingServlet.DO_FILTER);
        }
        finally {
            container.stop();
        }
    }

}
