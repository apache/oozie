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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.oozie.cli.OozieCLI;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.HeaderTestingVersionServlet;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.service.ForTestAuthorizationService;
import org.apache.oozie.service.ForTestWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;

import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 *
 */
public class TestAuthFilterAuthOozieClient extends XTestCase {
    private EmbeddedServletContainer container;

    protected String getContextURL() {
        return container.getContextURL();
    }

    protected URL createURL(String servletPath, String resource, Map<String, String> parameters) throws Exception {
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

    protected void runTest(Callable<Void> assertions, Configuration additionalConf) throws Exception {
        Services services = new Services();
        try {
            services.init();
            if (additionalConf != null) {
                for (Map.Entry<String, String> prop : additionalConf) {
                    Services.get().getConf().set(prop.getKey(), prop.getValue());
                }
            }
            Services.get().setService(ForTestAuthorizationService.class);
            Services.get().setService(ForTestWorkflowStoreService.class);
            Services.get().setService(MockDagEngineService.class);
            Services.get().setService(MockCoordinatorEngineService.class);
            container = new EmbeddedServletContainer("oozie");
            container.addServletEndpoint("/versions", HeaderTestingVersionServlet.class);
            String version = "/v" + XOozieClient.WS_PROTOCOL_VERSION;
            container.addServletEndpoint(version + "/admin/*", V1AdminServlet.class);
            container.addFilter("*", HostnameFilter.class);
            container.addFilter("/*", AuthFilter.class);
            container.start();
            assertions.call();
        }
        finally {
            if (container != null) {
                container.stop();
            }
            services.destroy();
            container = null;
        }
    }

    public static class Authenticator4Test extends PseudoAuthenticator {

        private static boolean USED = false;

        @Override
        public void authenticate(URL url, AuthenticatedURL.Token token) throws IOException, AuthenticationException {
            USED = true;
            super.authenticate(url, token);
        }
    }

    public void testClientWithAnonymous() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("oozie.authentication.simple.anonymous.allowed", "true");

        runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, conf);
    }

    public void testClientWithoutAnonymous() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("oozie.authentication.simple.anonymous.allowed", "false");

        runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, conf);
    }

    public void testClientWithCustomAuthenticator() throws Exception {
        setSystemProperty("authenticator.class", Authenticator4Test.class.getName());
        Configuration conf = new Configuration(false);
        conf.set("oozie.authentication.simple.anonymous.allowed", "false");

        Authenticator4Test.USED = false;
        runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, conf);
        assertTrue(Authenticator4Test.USED);
    }


    public void testClientAuthTokenCache() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("oozie.authentication.signature.secret", "secret");
        conf.set("oozie.authentication.simple.anonymous.allowed", "false");

        //not using cache
        AuthOozieClient.AUTH_TOKEN_CACHE_FILE.delete();
        assertFalse(AuthOozieClient.AUTH_TOKEN_CACHE_FILE.exists());
        runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, conf);
        assertFalse(AuthOozieClient.AUTH_TOKEN_CACHE_FILE.exists());

        //using cache
        setSystemProperty("oozie.auth.token.cache", "true");
        AuthOozieClient.AUTH_TOKEN_CACHE_FILE.delete();
        assertFalse(AuthOozieClient.AUTH_TOKEN_CACHE_FILE.exists());
        runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, conf);
        assertTrue(AuthOozieClient.AUTH_TOKEN_CACHE_FILE.exists());
        String currentCache = IOUtils.getReaderAsString(new FileReader(AuthOozieClient.AUTH_TOKEN_CACHE_FILE), -1);

        //re-using cache
        setSystemProperty("oozie.auth.token.cache", "true");
        runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, conf);
        assertTrue(AuthOozieClient.AUTH_TOKEN_CACHE_FILE.exists());
        String newCache = IOUtils.getReaderAsString(new FileReader(AuthOozieClient.AUTH_TOKEN_CACHE_FILE), -1);
        assertEquals(currentCache, newCache);
    }

    /**
     * Test authentication
     */
    public void testClientAuthMethod() throws Exception {

        runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[] { "admin", "-status", "-oozie",
                        oozieUrl, "-auth", "SIMPLE" };
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, null);
        // bad method
        runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[] { "admin", "-status", "-oozie",
                        oozieUrl, "-auth", "fake" };
                assertEquals(-1, new OozieCLI().run(args));
                return null;
            }
        }, null);
    }
}
