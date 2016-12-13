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

package org.apache.oozie.server;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 *  Server tests
 */
@RunWith(MockitoJUnitRunner.class)
public class TestSSLServerConnectorFactory {
    @Mock private SslContextFactory mockSSLContextFactory;
    @Mock private SSLServerConnectorFactory mockSSLServerConnectorFactory;
    @Mock private Server mockServer;
    @Mock private ServerConnector mockServerConnector;

    private Configuration testConfig;
    private SSLServerConnectorFactory sslServerConnectorFactory;

    @Before public void setUp() {
        testConfig = new Configuration();
        testConfig.set("oozie.https.truststore.file", "test_truststore_file");
        testConfig.set("oozie.https.truststore.pass", "trustpass");
        testConfig.set("oozie.https.keystore.file", "test_keystore_file");
        testConfig.set("oozie.https.keystore.pass", "keypass");
        testConfig.set("oozie.http.port", "11000");
        testConfig.set("oozie.http.request.header.size", "65536");
        testConfig.set("oozie.http.response.header.size", "65536");
        testConfig.set("oozie.https.include.protocols", "TLSv1,SSLv2Hello,TLSv1.1,TLSv1.2");
        testConfig.set("oozie.https.exclude.cipher.suites",
                "TLS_ECDHE_RSA_WITH_RC4_128_SHA,SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA,SSL_RSA_WITH_DES_CBC_SHA," +
                "SSL_DHE_RSA_WITH_DES_CBC_SHA,SSL_RSA_EXPORT_WITH_RC4_40_MD5,SSL_RSA_EXPORT_WITH_DES40_CBC_SHA," +
                "SSL_RSA_WITH_RC4_128_MD5");

        sslServerConnectorFactory = new SSLServerConnectorFactory(mockSSLContextFactory);
    }

    @After
    public void tearDown() {
        verify(mockSSLContextFactory).setTrustStorePath(anyString());
        verify(mockSSLContextFactory).setTrustStorePassword(anyString());
        verify(mockSSLContextFactory).setKeyStorePath(anyString());
        verify(mockSSLContextFactory).setKeyManagerPassword(anyString());
        verifyNoMoreInteractions(
                mockServerConnector,
                mockSSLServerConnectorFactory);
    }

    private void verifyDefaultExcludeCipherSuites() {
        verify(mockSSLContextFactory).setExcludeCipherSuites(
                "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
                "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA",
                "SSL_RSA_WITH_DES_CBC_SHA",
                "SSL_DHE_RSA_WITH_DES_CBC_SHA",
                "SSL_RSA_EXPORT_WITH_RC4_40_MD5",
                "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA",
                "SSL_RSA_WITH_RC4_128_MD5");
    }

    private void verifyDefaultIncludeProtocols() {
        verify(mockSSLContextFactory).setIncludeProtocols(
                "TLSv1",
                "SSLv2Hello",
                "TLSv1.1",
                "TLSv1.2");
    }

    @Test
    public void includeProtocolsHaveDefaultValues() throws Exception {
        sslServerConnectorFactory.createSecureServerConnector(42, testConfig, mockServer);

        verifyDefaultIncludeProtocols();
        verifyDefaultExcludeCipherSuites();
    }

    @Test
    public void includeProtocolsCanBeSetViaConfigFile() throws Exception {
        SSLServerConnectorFactory sslServerConnectorFactory = new SSLServerConnectorFactory(mockSSLContextFactory);
        testConfig.set("oozie.https.include.protocols", "TLSv1,TLSv1.2");
        sslServerConnectorFactory.createSecureServerConnector(42, testConfig, mockServer);

        verify(mockSSLContextFactory).setIncludeProtocols(
                "TLSv1",
                "TLSv1.2");
    }

    @Test
    public void excludeCipherSuitesHaveDefaultValues() throws Exception {
        sslServerConnectorFactory.createSecureServerConnector(42, testConfig, mockServer);

        verifyDefaultExcludeCipherSuites();
        verifyDefaultIncludeProtocols();
    }

    @Test
    public void excludeCipherSuitesCanBeSetViaConfigFile() throws Exception {
        testConfig.set("oozie.https.exclude.cipher.suites","TLS_ECDHE_RSA_WITH_RC4_128_SHA,SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA,"
                + "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA");

        sslServerConnectorFactory.createSecureServerConnector(42, testConfig, mockServer);

        verify(mockSSLContextFactory).setExcludeCipherSuites(
                "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
                "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA",
                "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA");
        verifyDefaultIncludeProtocols();
    }
}
