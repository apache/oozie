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

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
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
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 *
 */
public class TestAuthFilterAuthOozieClient extends XTestCase {
    private static final String SECRET = "secret";
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
        // This test requires a constant secret.
        // In Hadoop 2.5.0, you can set a secret string directly with oozie.authentication.signature.secret and the
        // AuthenticationFilter will use it.
        // In Hadoop 2.6.0 (HADOOP-10868), this was abstracted out to SecretProviders that have differnet implementations.  By
        // default, if a String was given for the secret, the StringSignerSecretProvider would be automatically used and
        // oozie.authentication.signature.secret would still be loaded.
        // In Hadoop 2.7.0 (HADOOP-11748), this automatic behavior was removed for security reasons, and the class was made package
        // private and moved to the hadoop-auth test artifact.  So, not only can we not simply set
        // oozie.authentication.signature.secret, but we also can't manually configure the StringSignerSecretProvider either.
        // However, Hadoop 2.7.0  (HADOOP-10670) also added a FileSignerSecretProvider, which we'll use if it exists
        try {
            if (Class.forName("org.apache.hadoop.security.authentication.util.FileSignerSecretProvider") != null) {
                String secretFile = getTestCaseConfDir() + "/auth-secret";
                conf.set("oozie.authentication.signature.secret.file", secretFile);
                FileWriter fw = null;
                try {
                    fw = new FileWriter(secretFile);
                    fw.write(SECRET);
                } finally {
                    if (fw != null) {
                        fw.close();
                    }
                }
            }
        } catch (ClassNotFoundException cnfe) {
            // ignore
        }
        conf.set("oozie.authentication.signature.secret", SECRET);
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

        //re-using cache with token that will expire within 5 minutes
        currentCache = writeTokenCache(System.currentTimeMillis() + 300000);
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
        newCache = IOUtils.getReaderAsString(new FileReader(AuthOozieClient.AUTH_TOKEN_CACHE_FILE), -1);
        assertFalse("Almost expired token should have been updated but was not", currentCache.equals(newCache));

        //re-using cache with expired token
        currentCache = writeTokenCache(System.currentTimeMillis() - 1000);
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
        newCache = IOUtils.getReaderAsString(new FileReader(AuthOozieClient.AUTH_TOKEN_CACHE_FILE), -1);
        assertFalse("Expired token should have been updated but was not", currentCache.equals(newCache));
    }

    private static String writeTokenCache(long expirationTime) throws Exception {
        AuthenticationToken authToken = new AuthenticationToken(getOozieUser(), getOozieUser(), "simple");
        authToken.setExpires(expirationTime);
        String signedTokenStr = computeSignature(SECRET.getBytes(Charset.forName("UTF-8")), authToken.toString());
        signedTokenStr = authToken.toString() + "&s=" + signedTokenStr;
        PrintWriter pw = new PrintWriter(AuthOozieClient.AUTH_TOKEN_CACHE_FILE);
        pw.write(signedTokenStr);
        pw.close();
        return signedTokenStr;
    }

    // Borrowed from org.apache.hadoop.security.authentication.util.Signer#computeSignature
    private static String computeSignature(byte[] secret, String str) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA");
        md.update(str.getBytes());
        md.update(secret);
        byte[] digest = md.digest();
        return new Base64(0).encodeToString(digest);
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
