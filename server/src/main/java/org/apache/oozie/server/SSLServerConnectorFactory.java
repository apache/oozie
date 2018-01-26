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


import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.ConfigurationService;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Factory that is used to configure SSL settings for the Oozie server.
 */
class SSLServerConnectorFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SSLServerConnectorFactory.class);
    public static final String OOZIE_HTTPS_KEYSTORE_PASS = "oozie.https.keystore.pass";
    public static final String OOZIE_HTTPS_KEYSTORE_FILE = "oozie.https.keystore.file";
    public static final String OOZIE_HTTPS_EXCLUDE_PROTOCOLS = "oozie.https.exclude.protocols";
    public static final String OOZIE_HTTPS_INCLUDE_PROTOCOLS = "oozie.https.include.protocols";
    public static final String OOZIE_HTTPS_INCLUDE_CIPHER_SUITES = "oozie.https.include.cipher.suites";
    public static final String OOZIE_HTTPS_EXCLUDE_CIPHER_SUITES = "oozie.https.exclude.cipher.suites";

    private SslContextFactory sslContextFactory;
    private Configuration conf;

    @Inject
    public SSLServerConnectorFactory(final SslContextFactory sslContextFactory) {
        this.sslContextFactory = Preconditions.checkNotNull(sslContextFactory,  "sslContextFactory is null");
    }

    /**
     *  Construct a ServerConnector object with SSL settings
     *
     *  @param oozieHttpsPort Oozie HTTPS port
     *  @param conf Oozie configuration
     *  @param server jetty Server which the connector is attached to
     *
     *  @return ServerConnector
    */
    public ServerConnector createSecureServerConnector(int oozieHttpsPort, Configuration conf, Server server) {
        this.conf = Preconditions.checkNotNull(conf, "conf is null");
        Preconditions.checkNotNull(server, "server is null");
        Preconditions.checkState(oozieHttpsPort >= 1 && oozieHttpsPort <= 65535,
                String.format("Invalid port number specified: \'%d\'. It should be between 1 and 65535.", oozieHttpsPort));

        setIncludeProtocols();
        setExcludeProtocols();

        setIncludeCipherSuites();
        setExludeCipherSuites();

        setKeyStoreFile();
        setKeystorePass();

        HttpConfiguration httpsConfiguration = getHttpsConfiguration();
        ServerConnector secureServerConnector = new ServerConnector(server,
                new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
                new HttpConnectionFactory(httpsConfiguration));

        secureServerConnector.setPort(oozieHttpsPort);

        LOG.info(String.format("Secure server connector created, listening on port %d", oozieHttpsPort));
        return secureServerConnector;
    }

    private void setExludeCipherSuites() {
        String excludeCipherList = conf.get(OOZIE_HTTPS_EXCLUDE_CIPHER_SUITES);
        String[] excludeCipherSuites = excludeCipherList.split(",");
        sslContextFactory.setExcludeCipherSuites(excludeCipherSuites);

        LOG.info(String.format("SSL context - excluding cipher suites: %s", Arrays.toString(excludeCipherSuites)));
    }

    private void setIncludeCipherSuites() {
        String includeCipherList = conf.get(OOZIE_HTTPS_INCLUDE_CIPHER_SUITES);
        if (includeCipherList == null || includeCipherList.isEmpty()) {
            return;
        }

        String[] includeCipherSuites = includeCipherList.split(",");
        sslContextFactory.setIncludeCipherSuites(includeCipherSuites);

        LOG.info(String.format("SSL context - including cipher suites: %s", Arrays.toString(includeCipherSuites)));
    }

    private void setIncludeProtocols() {
        String enabledProtocolsList = conf.get(OOZIE_HTTPS_INCLUDE_PROTOCOLS);
        String[] enabledProtocols = enabledProtocolsList.split(",");
        sslContextFactory.setIncludeProtocols(enabledProtocols);

        LOG.info(String.format("SSL context - including protocols: %s", Arrays.toString(enabledProtocols)));
    }

    private void setExcludeProtocols() {
        String excludedProtocolsList = conf.get(OOZIE_HTTPS_EXCLUDE_PROTOCOLS);
        if (excludedProtocolsList == null || excludedProtocolsList.isEmpty()) {
            return;
        }
        String[] excludedProtocols = excludedProtocolsList.split(",");
        sslContextFactory.setExcludeProtocols(excludedProtocols);
        LOG.info(String.format("SSL context - excluding protocols: %s", Arrays.toString(excludedProtocols)));
    }

    private void setKeystorePass() {
        String keystorePass = ConfigurationService.getPassword(conf, OOZIE_HTTPS_KEYSTORE_PASS);
        Preconditions.checkNotNull(keystorePass, "keystorePass is null");
        sslContextFactory.setKeyManagerPassword(keystorePass);
    }

    private void setKeyStoreFile() {
        String keystoreFile = conf.get(OOZIE_HTTPS_KEYSTORE_FILE);
        Preconditions.checkNotNull(keystoreFile, "keystoreFile is null");
        sslContextFactory.setKeyStorePath(keystoreFile);
    }

    private HttpConfiguration getHttpsConfiguration() {
        HttpConfiguration https = new HttpConfigurationWrapper(conf).getDefaultHttpConfiguration();
        https.setSecureScheme("https");
        https.addCustomizer(new SecureRequestCustomizer());
        return https;
    }
}
