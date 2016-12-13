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
    public static final String OOZIE_HTTPS_TRUSTSTORE_FILE = "oozie.https.truststore.file";
    public static final String OOZIE_HTTPS_TRUSTSTORE_PASS = "oozie.https.truststore.pass";
    public static final String OOZIE_HTTPS_KEYSTORE_PASS = "oozie.https.keystore.pass";
    public static final String OOZIE_HTTPS_KEYSTORE_FILE = "oozie.https.keystore.file";

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
        setCipherSuites();
        setTrustStorePath();
        setTrustStorePass();

        setKeyStoreFile();
        setKeystorePass();

        HttpConfiguration httpsConfiguration = getHttpsConfiguration();
        ServerConnector secureServerConnector = new ServerConnector(server,
                new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
                new HttpConnectionFactory(httpsConfiguration));

        secureServerConnector.setPort(oozieHttpsPort);

        LOG.info(String.format("Secure server connector created, listenning on port %d", oozieHttpsPort));
        return secureServerConnector;
    }

    private void setCipherSuites() {
        String excludeCipherList = conf.get("oozie.https.exclude.cipher.suites");
        String[] excludeCipherSuites = excludeCipherList.split(",");
        sslContextFactory.setExcludeCipherSuites(excludeCipherSuites);

        LOG.info(String.format("SSL context - excluding cipher suites: %s", Arrays.toString(excludeCipherSuites)));
    }

    private void setIncludeProtocols() {
        String enabledProtocolsList = conf.get("oozie.https.include.protocols");
        String[] enabledProtocols = enabledProtocolsList.split(",");
        sslContextFactory.setIncludeProtocols(enabledProtocols);

        LOG.info(String.format("SSL context - including protocols: %s", Arrays.toString(enabledProtocols)));
    }

    private void setTrustStorePath() {
        String trustStorePath = conf.get(OOZIE_HTTPS_TRUSTSTORE_FILE);
        Preconditions.checkNotNull(trustStorePath, "trustStorePath is null");
        sslContextFactory.setTrustStorePath(trustStorePath);
    }

    private void setTrustStorePass() {
        String trustStorePass = conf.get(OOZIE_HTTPS_TRUSTSTORE_PASS);
        Preconditions.checkNotNull(trustStorePass, "setTrustStorePass is null");
        sslContextFactory.setTrustStorePassword(trustStorePass);
    }

    private void setKeystorePass() {
        String keystorePass = conf.get(OOZIE_HTTPS_KEYSTORE_PASS);
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
