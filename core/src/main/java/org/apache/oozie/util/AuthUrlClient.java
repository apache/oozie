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


package org.apache.oozie.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.oozie.service.Services;

public class AuthUrlClient {

    static private Class<? extends Authenticator> AuthenticatorClass = null;

    static private String errorMsg = null;

    static {
        try {
            AuthenticatorClass = determineAuthenticatorClassType();
        }
        catch (Exception e) {
            errorMsg = e.getMessage();
        }
    }

    private static HttpURLConnection getConnection(URL url) throws IOException {
        AuthenticatedURL.Token token = new AuthenticatedURL.Token();
        HttpURLConnection conn;
        try {
            conn = new AuthenticatedURL(AuthenticatorClass.newInstance()).openConnection(url, token);
        }
        catch (AuthenticationException ex) {
            throw new IOException("Could not authenticate, " + ex.getMessage(), ex);
        }
        catch (InstantiationException ex) {
            throw new IOException("Could not authenticate, " + ex.getMessage(), ex);
        }
        catch (IllegalAccessException ex) {
            throw new IOException("Could not authenticate, " + ex.getMessage(), ex);
        }
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            throw new IOException("Unexpected response code [" + conn.getResponseCode() + "], message ["
                    + conn.getResponseMessage() + "]");
        }
        return conn;
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends Authenticator> determineAuthenticatorClassType() throws Exception {
        // Adapted from
        // org.apache.hadoop.security.authentication.server.AuthenticationFilter#init
        Class<? extends Authenticator> authClass;
        String authName = Services.get().getConf().get("oozie.authentication.type");
        String authClassName;
        if (authName == null) {
            throw new IOException("Authentication type must be specified: simple|kerberos|<class>");
        }
        authName = authName.trim();
        if (authName.equals("simple")) {
            authClassName = PseudoAuthenticator.class.getName();
        }
        else if (authName.equals("kerberos")) {
            authClassName = KerberosAuthenticator.class.getName();
        }
        else {
            authClassName = authName;
        }

        authClass = (Class<? extends Authenticator>) Thread.currentThread().getContextClassLoader()
                .loadClass(authClassName);
        return authClass;
    }

    /**
     * Calls other Oozie server over HTTP.
     *
     * @param server The URL of the other Oozie server
     * @return BufferedReader of inputstream.
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static BufferedReader callServer(String server) throws IOException {

        if (AuthenticatorClass == null) {
            throw new IOException(errorMsg);
        }

        final URL url = new URL(server);
        BufferedReader reader = null;
        try {
            reader = UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<BufferedReader>() {
                @Override
                public BufferedReader run() throws IOException {
                    HttpURLConnection conn = getConnection(url);
                    BufferedReader reader = null;
                    if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                        InputStream is = conn.getInputStream();
                        reader = new BufferedReader(new InputStreamReader(is));
                    }
                    return reader;
                }
            });
        }
        catch (InterruptedException ie) {
            throw new IOException(ie);
        }
        return reader;
    }

    public static String getQueryParamString(Map<String, String[]> params) {
        StringBuilder stringBuilder = new StringBuilder();
        if (params == null || params.isEmpty()) {
            return "";
        }
        for (String key : params.keySet()) {
            if (!key.isEmpty() && params.get(key).length > 0) {
                stringBuilder.append("&");
                String value = params.get(key)[0]; // We don't support multi value.
                stringBuilder.append(key);
                stringBuilder.append("=");
                stringBuilder.append(value);
            }
        }
        return stringBuilder.toString();
    }

}
