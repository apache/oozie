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

package org.apache.oozie.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;

/**
 * This subclass of {@link XOozieClient} supports Kerberos HTTP SPNEGO and simple authentication.
 */
public class AuthOozieClient extends XOozieClient {

    /**
     * Java system property to specify a custom Authenticator implementation.
     */
    public static final String AUTHENTICATOR_CLASS_SYS_PROP = "authenticator.class";

    /**
     * Java system property that, if set the authentication token will be cached in the user home directory in a hidden
     * file <code>.oozie-auth-token</code> with user read/write permissions only.
     */
    public static final String USE_AUTH_TOKEN_CACHE_SYS_PROP = "oozie.auth.token.cache";

    /**
     * File constant that defines the location of the authentication token cache file.
     * <p/>
     * It resolves to <code>${user.home}/.oozie-auth-token</code>.
     */
    public static final File AUTH_TOKEN_CACHE_FILE = new File(System.getProperty("user.home"), ".oozie-auth-token");

    public static enum AuthType {
        KERBEROS, SIMPLE
    }

    private String authOption = null;

    /**
     * Create an instance of the AuthOozieClient.
     *
     * @param oozieUrl the Oozie URL
     */
    public AuthOozieClient(String oozieUrl) {
        this(oozieUrl, null);
    }

    /**
     * Create an instance of the AuthOozieClient.
     *
     * @param oozieUrl the Oozie URL
     * @param authOption the auth option
     */
    public AuthOozieClient(String oozieUrl, String authOption) {
        super(oozieUrl);
        this.authOption = authOption;
    }

    /**
     * Create an authenticated connection to the Oozie server.
     * <p/>
     * It uses Hadoop-auth client authentication which by default supports
     * Kerberos HTTP SPNEGO, Pseudo/Simple and anonymous.
     * <p/>
     * if the Java system property {@link #USE_AUTH_TOKEN_CACHE_SYS_PROP} is set to true Hadoop-auth
     * authentication token will be cached/used in/from the '.oozie-auth-token' file in the user
     * home directory.
     *
     * @param url the URL to open a HTTP connection to.
     * @param method the HTTP method for the HTTP connection.
     * @return an authenticated connection to the Oozie server.
     * @throws IOException if an IO error occurred.
     * @throws OozieClientException if an oozie client error occurred.
     */
    @Override
    protected HttpURLConnection createConnection(URL url, String method) throws IOException, OozieClientException {
        boolean useAuthFile = System.getProperty(USE_AUTH_TOKEN_CACHE_SYS_PROP, "false").equalsIgnoreCase("true");
        AuthenticatedURL.Token readToken = new AuthenticatedURL.Token();
        AuthenticatedURL.Token currentToken = new AuthenticatedURL.Token();

        if (useAuthFile) {
            readToken = readAuthToken();
            if (readToken != null) {
                currentToken = new AuthenticatedURL.Token(readToken.toString());
            }
        }

        if (currentToken.isSet()) {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("OPTIONS");
            AuthenticatedURL.injectToken(conn, currentToken);
            if (conn.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
                AUTH_TOKEN_CACHE_FILE.delete();
                currentToken = new AuthenticatedURL.Token();
            }
        }

        if (!currentToken.isSet()) {
            Authenticator authenticator = getAuthenticator();
            try {
                new AuthenticatedURL(authenticator).openConnection(url, currentToken);
            }
            catch (AuthenticationException ex) {
                AUTH_TOKEN_CACHE_FILE.delete();
                throw new OozieClientException(OozieClientException.AUTHENTICATION,
                                               "Could not authenticate, " + ex.getMessage(), ex);
            }
        }
        if (useAuthFile && currentToken.isSet() && !currentToken.equals(readToken)) {
            writeAuthToken(currentToken);
        }
        HttpURLConnection conn = super.createConnection(url, method);
        AuthenticatedURL.injectToken(conn, currentToken);

        return conn;
    }


    /**
     * Read a authentication token cached in the user home directory.
     * <p/>
     *
     * @return the authentication token cached in the user home directory, NULL if none.
     */
    protected AuthenticatedURL.Token readAuthToken() {
        AuthenticatedURL.Token authToken = null;
        if (AUTH_TOKEN_CACHE_FILE.exists()) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(AUTH_TOKEN_CACHE_FILE));
                String line = reader.readLine();
                reader.close();
                if (line != null) {
                    authToken = new AuthenticatedURL.Token(line);
                }
            }
            catch (IOException ex) {
                //NOP
            }
        }
        return authToken;
    }

    /**
     * Write the current authentication token to the user home directory.authOption
     * <p/>
     * The file is written with user only read/write permissions.
     * <p/>
     * If the file cannot be updated or the user only ready/write permissions cannot be set the file is deleted.
     *
     * @param authToken the authentication token to cache.
     */
    protected void writeAuthToken(AuthenticatedURL.Token authToken) {
        try {
            Writer writer = new FileWriter(AUTH_TOKEN_CACHE_FILE);
            writer.write(authToken.toString());
            writer.close();
            // sets read-write permissions to owner only
            AUTH_TOKEN_CACHE_FILE.setReadable(false, false);
            AUTH_TOKEN_CACHE_FILE.setReadable(true, true);
            AUTH_TOKEN_CACHE_FILE.setWritable(true, true);
        }
        catch (IOException ioe) {
            // if case of any error we just delete the cache, if user-only
            // write permissions are not properly set a security exception
            // is thrown and the file will be deleted.
            AUTH_TOKEN_CACHE_FILE.delete();
        }
    }

    /**
     * Return the Hadoop-auth Authenticator to use.
     * <p/>
     * It first looks for value of command line option 'auth', if not set it continues to check
     * {@link #AUTHENTICATOR_CLASS_SYS_PROP} Java system property for Authenticator.
     * <p/>
     * It the value of the {@link #AUTHENTICATOR_CLASS_SYS_PROP} is not set it uses
     * Hadoop-auth <code>KerberosAuthenticator</code> which supports both Kerberos HTTP SPNEGO and Pseudo/simple
     * authentication.
     *
     * @return the Authenticator to use, <code>NULL</code> if none.
     *
     * @throws OozieClientException thrown if the authenticator could not be instantiated.
     */
    protected Authenticator getAuthenticator() throws OozieClientException {
        if (authOption != null) {
            try {
                Class<? extends Authenticator> authClass = getAuthenticators().get(authOption.toUpperCase());
                if (authClass == null) {
                    throw new OozieClientException(OozieClientException.AUTHENTICATION,
                            "Authenticator class not found [" + authClass + "]");
                }
                return authClass.newInstance();
            }
            catch (IllegalArgumentException iae) {
                throw new OozieClientException(OozieClientException.AUTHENTICATION, "Invalid options provided for auth: " + authOption
                        + ", (" + AuthType.KERBEROS + " or " + AuthType.SIMPLE + " expected.)");
            }
            catch (InstantiationException ex) {
                throw new OozieClientException(OozieClientException.AUTHENTICATION,
                        "Could not instantiate Authenticator for option [" + authOption + "], " +
                        ex.getMessage(), ex);
            }
            catch (IllegalAccessException ex) {
                throw new OozieClientException(OozieClientException.AUTHENTICATION,
                        "Could not instantiate Authenticator for option [" + authOption + "], " +
                        ex.getMessage(), ex);
            }

        }

        String className = System.getProperty(AUTHENTICATOR_CLASS_SYS_PROP, KerberosAuthenticator.class.getName());
        if (className != null) {
            try {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                Class<? extends Object> klass = (cl != null) ? cl.loadClass(className) :
                    getClass().getClassLoader().loadClass(className);
                if (klass == null) {
                    throw new OozieClientException(OozieClientException.AUTHENTICATION,
                            "Authenticator class not found [" + className + "]");
                }
                return (Authenticator) klass.newInstance();
            }
            catch (Exception ex) {
                throw new OozieClientException(OozieClientException.AUTHENTICATION,
                                               "Could not instantiate Authenticator [" + className + "], " +
                                               ex.getMessage(), ex);
            }
        }
        else {
            throw new OozieClientException(OozieClientException.AUTHENTICATION,
                                           "Authenticator class not found [" + className + "]");
        }
    }

    /**
     * Get the map for classes of Authenticator.
     * Default values are:
     * null -> KerberosAuthenticator
     * SIMPLE -> PseudoAuthenticator
     * KERBEROS -> KerberosAuthenticator
     *
     * @return the map for classes of Authenticator
     * @throws OozieClientException
     */
    protected Map<String, Class<? extends Authenticator>> getAuthenticators() {
        Map<String, Class<? extends Authenticator>> authClasses = new HashMap<String, Class<? extends Authenticator>>();
        authClasses.put(AuthType.KERBEROS.toString(), KerberosAuthenticator.class);
        authClasses.put(AuthType.SIMPLE.toString(), PseudoAuthenticator.class);
        authClasses.put(null, KerberosAuthenticator.class);
        return authClasses;
    }

    /**
     * Get authOption
     *
     * @return the authOption
     */
    public String getAuthOption() {
        return authOption;
    }

}
