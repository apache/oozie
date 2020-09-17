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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.Base64;

/**
 *
 */
public class TestAuthFilterAuthOozieClient extends XTestCase {
    private static final String SECRET = "secret";
    private EmbeddedServletContainer container;
    private int embeddedServletContainerPort;

    public TestAuthFilterAuthOozieClient() {
        this.embeddedServletContainerPort = getFreePort();
    }

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
                sb.append(separator).append(URLEncoder.encode(param.getKey(), StandardCharsets.UTF_8.name())).append("=")
                        .append(URLEncoder.encode(param.getValue(), StandardCharsets.UTF_8.name()));
                separator = "&";
            }
        }
        return new URL(sb.toString());
    }

    protected File runTest(Callable<Void> assertions, Configuration additionalConf, String contextPath)
            throws Exception {
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
            // the embeddedServletContainer must be instantiated with the same port. On one hand, when testing the
            // authCache with the same oozieUrl, this will work. On the other hand, when testing multiple authCache with
            // the different oozieUrls, the cacheFileName( container.getContextUrl() ) also will be different.
            container = new EmbeddedServletContainer(contextPath, embeddedServletContainerPort);
            container.addServletEndpoint("/versions", HeaderTestingVersionServlet.class);
            String version = "/v" + XOozieClient.WS_PROTOCOL_VERSION;
            container.addServletEndpoint(version + "/admin/*", V1AdminServlet.class);
            container.addFilter("/*", HostnameFilter.class);
            container.addFilter("/*", AuthFilter.class);
            container.addFilter("/*", HttpResponseHeaderFilter.class);
            container.start();
            assertions.call();
            return getCacheFile(container.getContextURL());
        } finally {
            if (container != null) {
                container.stop();
            }
            services.destroy();
            container = null;
        }
    }

    private File getCacheFile(String oozieUrl) {
        AuthOozieClient authOozieClient = new AuthOozieClient(oozieUrl);
        String filename = authOozieClient.getAuthCacheFileName(oozieUrl);
        return new File(System.getProperty("user.home"), filename);
    }

    /**
     * get free port
     * @return
     */
    private int getFreePort() {
        Socket socket = new Socket();
        InetSocketAddress inetAddress = new InetSocketAddress(0);
        try {
            socket.bind(inetAddress);
            if (null == socket || socket.isClosed()) {
                return -1;
            }

            int port = socket.getLocalPort();
            socket.close();
            return port;
        } catch (IOException e) {
            System.err.println("Failed to get system free port, caused by: " + e.getMessage());
        }
        return -1;
    }

    protected File runTest(Callable<Void> assertions, Configuration additionalConf)
            throws Exception {
        return runTest(assertions, additionalConf, "oozie");
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
        Configuration conf = getAuthenticationConf();

        //not using cache
        File cacheFile = runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, conf);
        assertFalse(cacheFile.exists());

        //using cache
        setSystemProperty("oozie.auth.token.cache", "true");
        cacheFile.delete();
        assertFalse(cacheFile.exists());
        cacheFile = runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, conf);
        assertTrue(cacheFile.exists());
        String currentCache = IOUtils.getReaderAsString(new InputStreamReader(new FileInputStream(cacheFile),
                StandardCharsets.UTF_8), -1);

        //re-using cache
        setSystemProperty("oozie.auth.token.cache", "true");
        cacheFile = runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, conf);
        assertTrue(cacheFile.exists());
        String newCache = IOUtils.getReaderAsString(new InputStreamReader(new FileInputStream(cacheFile),
                StandardCharsets.UTF_8), -1);
        assertEquals(currentCache, newCache);

        //re-using cache with token that will expire within 5 minutes
        currentCache = writeTokenCache(System.currentTimeMillis() + 300000, cacheFile);
        setSystemProperty("oozie.auth.token.cache", "true");
        cacheFile = runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, conf);
        assertTrue(cacheFile.exists());
        newCache = IOUtils.getReaderAsString(new InputStreamReader(new FileInputStream(cacheFile),
                StandardCharsets.UTF_8), -1);
        assertFalse("Almost expired token should have been updated but was not", currentCache.equals(newCache));

        //re-using cache with expired token
        currentCache = writeTokenCache(System.currentTimeMillis() - 1000, cacheFile);
        setSystemProperty("oozie.auth.token.cache", "true");
        cacheFile = runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[]{"admin", "-status", "-oozie", oozieUrl};
                assertEquals(0, new OozieCLI().run(args));
                return null;
            }
        }, conf);
        assertTrue(cacheFile.exists());
        newCache = IOUtils.getReaderAsString(new InputStreamReader(new FileInputStream(cacheFile),
                StandardCharsets.UTF_8), -1);
        assertFalse("Expired token should have been updated but was not", currentCache.equals(newCache));

        setSystemProperty("oozie.auth.token.cache", "true");
        cacheFile.delete();
        assertFalse(cacheFile.exists());
    }

    // the authToken will be stored in the different files when the oozieUrl is different.
    // the authTokenCache filename is definded by the oozieUrl.
    // this will work when the oozieClient connects to the different cluster.
    public void testMultipleClientAuthTokenCache() throws Exception {
        Configuration conf = getAuthenticationConf();
        setSystemProperty("oozie.auth.token.cache", "true");

        File cacheFile_1 = serverRunTest(conf, "oozie_1");

        // with the same oozie host and the same contextPath
        File cacheFile_2 = serverRunTest(conf, "oozie_1");

        String currentCache_1 = IOUtils.getReaderAsString(new InputStreamReader(new FileInputStream(cacheFile_1),
                StandardCharsets.UTF_8), -1);
        String currentCache_2 = IOUtils.getReaderAsString(new InputStreamReader(new FileInputStream(cacheFile_2),
                StandardCharsets.UTF_8), -1);
        assertEquals("AuthTokenCache with the same oozieUrl should be same but was not", currentCache_1, currentCache_2);

        assertTrue("The cacheFile_2 file should exist but was not", cacheFile_2.exists());
        assertTrue("The cacheFile_1 file should exist but was not", cacheFile_1.exists());

        // with the same oozie host and with the different contextPath
        File cacheFile_3 = serverRunTest(conf, "oozie_3");

        // verify that the cacheFile will not be deleted
        assertTrue("The cacheFile_3 file should exist but was not", cacheFile_3.exists());
        assertTrue("The cacheFile_1 file should exist but was not", cacheFile_1.exists());

        String currentCache_3 = IOUtils.getReaderAsString(new InputStreamReader(new FileInputStream(cacheFile_3),
                StandardCharsets.UTF_8), -1);
        assertNotSame("AuthTokenCache with different oozieUrls should be different but was not", currentCache_1, currentCache_3);

        // with the different oozie host and the different contextPath, this request will fail
        File cacheFile_4 = runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[] { "admin", "-status", "-oozie",  oozieUrl + "/test"};
                assertNotSame("The request with the no existing url will fail but was not", 0, new OozieCLI().run(args));
                return null;
            }
        }, conf, "oozie_4");

        assertFalse("The cache can't exist when the request with the not existing url", cacheFile_4.exists());

        // remove the cache files
        cacheFile_2.delete();
        assertFalse("CacheFile_2 should not exist but was not", cacheFile_2.exists());
        assertFalse("CacheFile_1 should not exist but was not", cacheFile_1.exists());
        cacheFile_3.delete();
        assertFalse("CacheFile_3 should not exist but was not", cacheFile_3.exists());
    }

    private File serverRunTest(Configuration conf, String contextPath) throws Exception {
        return runTest(new Callable<Void>() {
            public Void call() throws Exception {
                String oozieUrl = getContextURL();
                String[] args = new String[] { "admin", "-status", "-oozie", oozieUrl };
                assertEquals("The request will be success but was not", 0, new OozieCLI().run(args));
                return null;
            }
        }, conf, contextPath);
    }

    private static String writeTokenCache(long expirationTime, File cacheFile) throws Exception {
        AuthenticationToken authToken = new AuthenticationToken(getOozieUser(), getOozieUser(), "simple");
        authToken.setExpires(expirationTime);
        String signedTokenStr = computeSignature(SECRET.getBytes(StandardCharsets.UTF_8), authToken.toString());
        signedTokenStr = authToken.toString() + "&s=" + signedTokenStr;
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(cacheFile),
                StandardCharsets.UTF_8));
        pw.write(signedTokenStr);
        pw.close();
        return signedTokenStr;
    }

    // Borrowed from org.apache.hadoop.security.authentication.util.Signer#computeSignature
    private static String computeSignature(byte[] secret, String str) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA");
        md.update(str.getBytes(StandardCharsets.UTF_8));
        md.update(secret);
        byte[] digest = md.digest();
        return Base64.getEncoder().encodeToString(digest);
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

    private Configuration getAuthenticationConf() {
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
                Writer fw =  new PrintWriter(new OutputStreamWriter(new FileOutputStream(secretFile),
                        StandardCharsets.UTF_8));
                fw.write(SECRET);
            }
        } catch (Exception cnfe) {
            // ignore
        }
        conf.set("oozie.authentication.signature.secret", SECRET);
        conf.set("oozie.authentication.simple.anonymous.allowed", "false");
        return conf;
    }
}
