/*
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

package org.apache.oozie.client;

import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

public final class InsecureConnectionHelper {
    private static final ConnectionConfigurator INSECURE_CONN_CONFIGURATOR = httpURLConnection -> {
        try {
            if (httpURLConnection instanceof HttpsURLConnection) {
                configureInsecureConnection((HttpsURLConnection) httpURLConnection);
            }
        } catch (Exception e) {
            throw new RuntimeException("Configuring HttpsUrlConnection failed", e);
        }
        return httpURLConnection;
    };

    private static final HostnameVerifier INSECURE_HOSTNAME_VERIFIER =  (s, session) -> true;

    private static final TrustManager[] TRUST_ALL_CERTS_MANAGER = new TrustManager[] {
            new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
                public void checkClientTrusted(
                        X509Certificate[] certs, String authType) {
                }
                public void checkServerTrusted(
                        X509Certificate[] certs, String authType) {
                }
            }
    };

    private static final SSLSocketFactory INSECURE_SSL_SOCKET_FACTORY;

    static {
        try {
            SSLContext sslcontext = SSLContext.getInstance("TLS");
            sslcontext.init(null, TRUST_ALL_CERTS_MANAGER, null);
            INSECURE_SSL_SOCKET_FACTORY = sslcontext.getSocketFactory();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the insecure SSL socket factory", e);
        }
    }

    private InsecureConnectionHelper() {} // no instances

    public static void configureInsecureConnection(HttpsURLConnection httpsURLConnection) {
        httpsURLConnection.setHostnameVerifier(INSECURE_HOSTNAME_VERIFIER);
        httpsURLConnection.setSSLSocketFactory(INSECURE_SSL_SOCKET_FACTORY);
    }

    public static void configureInsecureAuthenticator(Authenticator authenticator) {
        authenticator.setConnectionConfigurator(INSECURE_CONN_CONFIGURATOR);
    }
}
