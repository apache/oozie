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
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import static org.apache.oozie.server.HttpConfigurationWrapper.OOZIE_HTTP_REQUEST_HEADER_SIZE;
import static org.apache.oozie.server.HttpConfigurationWrapper.OOZIE_HTTP_RESPONSE_HEADER_SIZE;
import static org.apache.oozie.server.SSLServerConnectorFactory.OOZIE_HTTPS_EXCLUDE_CIPHER_SUITES;
import static org.apache.oozie.server.SSLServerConnectorFactory.OOZIE_HTTPS_EXCLUDE_PROTOCOLS;
import static org.apache.oozie.server.SSLServerConnectorFactory.OOZIE_HTTPS_INCLUDE_CIPHER_SUITES;
import static org.apache.oozie.server.SSLServerConnectorFactory.OOZIE_HTTPS_INCLUDE_PROTOCOLS;
import static org.apache.oozie.server.SSLServerConnectorFactory.OOZIE_HTTPS_KEYSTORE_FILE;
import static org.apache.oozie.server.SSLServerConnectorFactory.OOZIE_HTTPS_KEYSTORE_PASS;
import static org.apache.oozie.util.ConfigUtils.OOZIE_HTTP_PORT;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 *  Server tests
 */
@RunWith(MockitoJUnitRunner.class)
public class TestSSLServerConnectorFactory {
    @Mock private SslContextFactory mockSSLContextFactory;
    @Mock private SSLServerConnectorFactory mockSSLServerConnectorFactory;
    @Spy  private Server mockServer;
    @Mock private ServerConnector mockServerConnector;

    private Configuration testConfig;
    private SSLServerConnectorFactory sslServerConnectorFactory;

    @Before public void setUp() {
        testConfig = new Configuration();
        testConfig.set(OOZIE_HTTPS_KEYSTORE_FILE, "test_keystore_file");
        testConfig.set(OOZIE_HTTPS_KEYSTORE_PASS, "keypass");
        testConfig.set(OOZIE_HTTP_PORT, "11000");
        testConfig.set(OOZIE_HTTP_REQUEST_HEADER_SIZE, "65536");
        testConfig.set(OOZIE_HTTP_RESPONSE_HEADER_SIZE, "65536");
        testConfig.set(OOZIE_HTTPS_INCLUDE_PROTOCOLS, "TLSv1,SSLv2Hello,TLSv1.1,TLSv1.2");
        testConfig.set(OOZIE_HTTPS_EXCLUDE_PROTOCOLS, "");
        testConfig.set(OOZIE_HTTPS_EXCLUDE_CIPHER_SUITES,
                "TLS_ECDHE_RSA_WITH_RC4_128_SHA,SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA,SSL_RSA_WITH_DES_CBC_SHA," +
                "SSL_DHE_RSA_WITH_DES_CBC_SHA,SSL_RSA_EXPORT_WITH_RC4_40_MD5,SSL_RSA_EXPORT_WITH_DES40_CBC_SHA," +
                "SSL_RSA_WITH_RC4_128_MD5");
        testConfig.set(OOZIE_HTTPS_INCLUDE_CIPHER_SUITES, "");
        sslServerConnectorFactory = new SSLServerConnectorFactory(mockSSLContextFactory);
    }

    @After
    public void tearDown() {
        verify(mockSSLContextFactory).setKeyStorePath(anyString());
        verify(mockSSLContextFactory).setKeyManagerPassword(anyString());
        verifyNoMoreInteractions(
                mockServerConnector,
                mockSSLServerConnectorFactory);
    }

    @Test
    public void includeProtocolsCanBeSetViaConfig() throws Exception {
        SSLServerConnectorFactory sslServerConnectorFactory = new SSLServerConnectorFactory(mockSSLContextFactory);
        testConfig.set(OOZIE_HTTPS_INCLUDE_PROTOCOLS, "TLSv1,TLSv1.2");
        sslServerConnectorFactory.createSecureServerConnector(42, testConfig, mockServer);

        verify(mockSSLContextFactory).setIncludeProtocols(
                "TLSv1",
                "TLSv1.2");
    }

    @Test
    public void emptyExcludeProtocolsAreNotSet() throws Exception {
        sslServerConnectorFactory.createSecureServerConnector(42, testConfig, mockServer);
        verify(mockSSLContextFactory, never()).setExcludeProtocols(anyString());
    }

    @Test
    public void excludeProtocolsCanBeSetViaConfig() throws Exception {
        SSLServerConnectorFactory sslServerConnectorFactory = new SSLServerConnectorFactory(mockSSLContextFactory);
        testConfig.set(OOZIE_HTTPS_INCLUDE_PROTOCOLS, "TLSv1,TLSv1.2");
        testConfig.set(OOZIE_HTTPS_EXCLUDE_PROTOCOLS, "TLSv1");
        sslServerConnectorFactory.createSecureServerConnector(42, testConfig, mockServer);

        verify(mockSSLContextFactory).setIncludeProtocols(
                "TLSv1",
                "TLSv1.2");

        verify(mockSSLContextFactory).setExcludeProtocols(
                "TLSv1");
    }

    @Test
    public void emptyIncludeCipherSuitesAreNotSet() throws Exception {
        sslServerConnectorFactory.createSecureServerConnector(42, testConfig, mockServer);
        verify(mockSSLContextFactory, never()).setIncludeCipherSuites(anyString());
    }

    @Test
    public void includeCipherSuitesCanBeSetViaConfig() throws Exception {
        testConfig.set(OOZIE_HTTPS_INCLUDE_CIPHER_SUITES, "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA");

        sslServerConnectorFactory.createSecureServerConnector(42, testConfig, mockServer);
        verify(mockSSLContextFactory).setIncludeCipherSuites("SSL_RSA_EXPORT_WITH_DES40_CBC_SHA");
    }


    @Test
    public void excludeCipherSuitesCanBeSetViaConfig() throws Exception {
        testConfig.set(OOZIE_HTTPS_EXCLUDE_CIPHER_SUITES, "TLS_ECDHE_RSA_WITH_RC4_128_SHA,"
                + "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA,SSL_RSA_EXPORT_WITH_DES40_CBC_SHA");

        sslServerConnectorFactory.createSecureServerConnector(42, testConfig, mockServer);

        verify(mockSSLContextFactory).setExcludeCipherSuites(
                "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
                "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA",
                "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA");
    }
}
