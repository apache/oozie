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
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 *  Server tests
 */
@RunWith(MockitoJUnitRunner.class)
public class TestEmbeddedOozieServer {
    @Mock private JspHandler mockJspHandler;
    @Mock private Services mockServices;
    @Mock private SslContextFactory mockSSLContextFactory;
    @Mock private SSLServerConnectorFactory mockSSLServerConnectorFactory;
    @Spy private Server mockServer;
    @Mock private ServerConnector mockServerConnector;
    @Mock private ConfigurationService mockConfigService;
    @Mock private Configuration mockConfiguration;
    @Mock private RewriteHandler mockOozieRewriteHandler;
    @Mock private WebAppContext servletContextHandler;
    @Mock private ServletMapper oozieServletMapper;
    @Mock private FilterMapper oozieFilterMapper;
    @Mock private ConstraintSecurityHandler constraintSecurityHandler;
    private EmbeddedOozieServer embeddedOozieServer;
    private String confTruststoreFile = "oozie.truststore";


    @Before public void setUp() throws IOException {
        embeddedOozieServer = new EmbeddedOozieServer(mockServer, mockJspHandler, mockServices, mockSSLServerConnectorFactory,
                mockOozieRewriteHandler, servletContextHandler, oozieServletMapper, oozieFilterMapper, constraintSecurityHandler);

        doReturn("11000").when(mockConfiguration).get("oozie.http.port");
        doReturn("11443").when(mockConfiguration).get("oozie.https.port");
        doReturn("65536").when(mockConfiguration).get("oozie.http.request.header.size");
        doReturn("65536").when(mockConfiguration).get("oozie.http.response.header.size");
        doReturn("42").when(mockConfiguration).get("oozie.server.threadpool.max.threads");
        doReturn("https://localhost:11443/oozie").when(mockConfiguration).get("oozie.base.url");
        doReturn(mockConfiguration).when(mockConfigService).getConf();
        doReturn(mockConfigService).when(mockServices).get(ConfigurationService.class);
        doNothing().when(mockServer).setConnectors((Connector[]) anyObject());
        doNothing().when(mockServer).setHandler((Handler) anyObject());
        doReturn(new Handler[0]).when(mockOozieRewriteHandler).getChildHandlers();
        doReturn(new Handler[0]).when(servletContextHandler).getChildHandlers();
        doReturn(new Handler[0]).when(constraintSecurityHandler).getChildHandlers();
        doReturn(confTruststoreFile).when(mockConfiguration).get(EmbeddedOozieServer.OOZIE_HTTPS_TRUSTSTORE_FILE);
        System.clearProperty(EmbeddedOozieServer.TRUSTSTORE_PATH_SYSTEM_PROPERTY);
    }

    @After public void tearDown() {
        System.clearProperty(EmbeddedOozieServer.TRUSTSTORE_PATH_SYSTEM_PROPERTY);

        verify(mockServices).get(ConfigurationService.class);

        verifyNoMoreInteractions(
                mockJspHandler,
                mockServices,
                mockServerConnector,
                mockSSLServerConnectorFactory);
    }

    @Test
    public void testServerSetup() throws Exception {
        doReturn("false").when(mockConfiguration).get("oozie.https.enabled");

        embeddedOozieServer.setup();
        verify(mockJspHandler).setupWebAppContext(isA(WebAppContext.class));

        // trustore parameters will have to be set even in case of an insecure setup
        Assert.assertEquals(confTruststoreFile, System.getProperty("javax.net.ssl.trustStore"));
    }

    /**
     * test case for when the trustore path is set via system property
     * expected result: the path is used from the system property and the value is not even retrieved from the config file
     */
    @Test
    public void testServerSetupTruststorePathSetViaSystemProperty() throws Exception {
        final String truststorePath2 = "truststore.jks";
        doReturn(String.valueOf(false)).when(mockConfiguration).get("oozie.https.enabled");
        System.setProperty(EmbeddedOozieServer.TRUSTSTORE_PATH_SYSTEM_PROPERTY, truststorePath2);

        embeddedOozieServer.setup();
        verify(mockJspHandler).setupWebAppContext(isA(WebAppContext.class));

        Assert.assertEquals(truststorePath2, System.getProperty("javax.net.ssl.trustStore"));
        verify(mockConfiguration, never()).get(EmbeddedOozieServer.OOZIE_HTTPS_TRUSTSTORE_FILE);
    }


    @Test
    public void testSecureServerSetup() throws Exception {
        doReturn("true").when(mockConfiguration).get("oozie.https.enabled");

        ServerConnector mockSecuredServerConnector = new ServerConnector(embeddedOozieServer.server);
        doReturn(mockSecuredServerConnector)
                .when(mockSSLServerConnectorFactory)
                .createSecureServerConnector(anyInt(), any(Configuration.class), any(Server.class));

        embeddedOozieServer.setup();

        verify(mockJspHandler).setupWebAppContext(isA(WebAppContext.class));
        verify(mockSSLServerConnectorFactory).createSecureServerConnector(
                isA(Integer.class), isA(Configuration.class), isA(Server.class));
        Assert.assertEquals(confTruststoreFile, System.getProperty("javax.net.ssl.trustStore"));
    }

    @Test(expected=NumberFormatException.class)
    public void numberFormatExceptionThrownWithInvalidHttpPort() throws ServiceException, IOException, URISyntaxException {
        doReturn("INVALID_PORT").when(mockConfiguration).get("oozie.http.port");
        embeddedOozieServer.setup();
    }
}
