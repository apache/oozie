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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Base64;

import javax.net.ssl.HttpsURLConnection;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import org.apache.commons.io.FilenameUtils;
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

    public static final int AUTH_TOKEN_CACHE_FILENAME_MAXLENGTH = 255;

    public enum AuthType {
        KERBEROS, SIMPLE, BASIC
    }

    private String authOption = null;
    private boolean isInsecureConnEnabled = false;

    /**
     * authTokenCacheFile defines the location of the authentication token cache file.
     * <p>
     * It resolves to <code>${user.home}/.oozie-auth-token-Base64(${oozieUrl})</code>.
     */
    private final File authTokenCacheFile;

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
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "FilenameUtils is used to filter user input. JDK8+ is used.")
    public AuthOozieClient(String oozieUrl, String authOption) {
        this(oozieUrl, authOption, false);
    }

    /**
     * Create an instance of the AuthOozieClient.
     *
     * @param oozieUrl the Oozie URL
     * @param authOption the auth option
     * @param isInsecureConnEnabled option for handling potential certificate errors
     */
    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "FilenameUtils is used to filter user input. JDK8+ is used.")
    public AuthOozieClient(String oozieUrl, String authOption, boolean isInsecureConnEnabled) {
        super(oozieUrl);
        this.authOption = authOption;
        this.isInsecureConnEnabled = isInsecureConnEnabled;
        String filename = getAuthCacheFileName(oozieUrl);
        // just to filter user input
        authTokenCacheFile = new File(System.getProperty("user.home"), FilenameUtils.getName(filename));
        if (filename.length() >= AUTH_TOKEN_CACHE_FILENAME_MAXLENGTH && authTokenCacheFile.exists()) {
            System.out.println("Warn: the same Oozie auth cache filename exists, filename=" + filename);
        }
    }

    @VisibleForTesting
    public String getAuthCacheFileName(String oozieUrl) {
        String encodeBase64OozieUrl = Base64.getEncoder().encodeToString(oozieUrl.getBytes(StandardCharsets.UTF_8));
        String filename = ".oozie-auth-token-" + encodeBase64OozieUrl;
        if (filename.length() >= AUTH_TOKEN_CACHE_FILENAME_MAXLENGTH) {
            filename = filename.substring(0, AUTH_TOKEN_CACHE_FILENAME_MAXLENGTH);
        }
        return filename;
    }

    private void configureConnection(HttpURLConnection httpURLConnection) {
        if (isInsecureConnEnabled && httpURLConnection instanceof HttpsURLConnection) {
            InsecureConnectionHelper.configureInsecureConnection((HttpsURLConnection) httpURLConnection);
        }
    }

    private void configureAuthenticator(Authenticator authenticator) {
        if (isInsecureConnEnabled) {
            InsecureConnectionHelper.configureInsecureAuthenticator(authenticator);
        }
    }

    /**
     * Create an authenticated connection to the Oozie server.
     * <p>
     * It uses Hadoop-auth client authentication which by default supports
     * Kerberos HTTP SPNEGO, Pseudo/Simple and anonymous.
     * <p>
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
        AuthenticatedURL.Token readToken = null;
        AuthenticatedURL.Token currentToken;

        // Read the token in from the file
        if (useAuthFile) {
            readToken = readAuthToken();
        }
        if (readToken == null) {
            currentToken = new AuthenticatedURL.Token();
        } else {
            currentToken = new AuthenticatedURL.Token(readToken.toString());
        }

        // To prevent rare race conditions and to save a call to the Server, lets check the token's expiration time locally, and
        // consider it expired if its expiration time has passed or will pass in the next 5 minutes (or if there's a problem parsing
        // it)
        if (currentToken.isSet()) {
            long expires = getExpirationTime(currentToken);
            if (expires < System.currentTimeMillis() + 300000) {
                if (useAuthFile) {
                    authTokenCacheFile.delete();
                }
                currentToken = new AuthenticatedURL.Token();
            }
        }

        // If we have a token, double check with the Server to make sure it hasn't expired yet
        if (currentToken.isSet()) {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            configureConnection(conn);
            conn.setRequestMethod("OPTIONS");
            AuthenticatedURL.injectToken(conn, currentToken);
            if (conn.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED
                    || conn.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN) {
                if (useAuthFile) {
                    authTokenCacheFile.delete();
                }
                currentToken = new AuthenticatedURL.Token();
            } else {
                // After HADOOP-10301, with Kerberos the above token expiration check will now send 200 even with an expired token
                // if you still have valid Kerberos credentials.  Previously, it would send 401 so the client knows that it needs to
                // use the KerberosAuthenticator to get a new token.  Now, it may even provide a token back from this call, so we
                // need to check for a new token and update ours.  If no new token was given and we got a 20X code, this will do a
                // no-op.
                // With Pseudo, the above token expiration check will now send 403 instead of the 401; we're now checking for either
                // response code above.  However, unlike with Kerberos, Pseudo doesn't give us a new token here; we'll have to get
                // one later.
                try {
                    AuthenticatedURL.extractToken(conn, currentToken);
                } catch (AuthenticationException ex) {
                    if (useAuthFile) {
                        authTokenCacheFile.delete();
                    }
                    currentToken = new AuthenticatedURL.Token();
                }
            }
        }

        // If we didn't have a token, or it had expired, let's get a new one from the Server using the configured Authenticator
        if (!currentToken.isSet()) {
            Authenticator authenticator = getAuthenticator();
            configureAuthenticator(authenticator);
            try {
                authenticator.authenticate(url, currentToken);
            }
            catch (AuthenticationException ex) {
                if (useAuthFile) {
                    authTokenCacheFile.delete();
                }
                throw new OozieClientException(OozieClientException.AUTHENTICATION,
                                               "Could not authenticate, " + ex.getMessage(), ex);
            }
        }

        // If we got a new token, save it to the cache file
        // For comparison of currentToken and readToken, please see the details of OOZIE-3396
        // Here, because of Hadoop AuthenticatedURL.Token don't override the equals() method,
        // we have to compare the token.toString()
        if (useAuthFile && currentToken.isSet() &&
                (readToken == null || !currentToken.toString().equals(readToken.toString()))) {
            writeAuthToken(currentToken);
        }

        // Now create a connection using the token and return it to the caller
        HttpURLConnection conn = super.createConnection(url, method);
        configureConnection(conn);
        AuthenticatedURL.injectToken(conn, currentToken);
        return conn;
    }

    private static long getExpirationTime(AuthenticatedURL.Token token) {
        long expires = 0L;
        String[] splits = token.toString().split("&");
        for (String split : splits) {
            if (split.startsWith("e=")) {
                try {
                    expires = Long.parseLong(split.substring(2));
                } catch (Exception e) {
                    // token is somehow invalid, assume it expired already
                    break;
                }
            }
        }
        return expires;
    }

    /**
     * Read a authentication token cached in the user home directory.
     * <p>
     *
     * @return the authentication token cached in the user home directory, NULL if none.
     */
    protected AuthenticatedURL.Token readAuthToken() {
        AuthenticatedURL.Token authToken = null;
        if (authTokenCacheFile.exists()) {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(authTokenCacheFile),
                        StandardCharsets.UTF_8));
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
     * <p>
     * The file is written with user only read/write permissions.
     * <p>
     * If the file cannot be updated or the user only ready/write permissions cannot be set the file is deleted.
     *
     * @param authToken the authentication token to cache.
     */
    protected void writeAuthToken(AuthenticatedURL.Token authToken) {
        try {
            String jvmName = ManagementFactory.getRuntimeMXBean().getName();
            File tmpTokenFile = Files.createTempFile(new File(System.getProperty("user.home")).toPath(), ".oozie-auth-token", jvmName + "tmp").toFile();
            // just to be safe, if something goes wrong delete tmp file eventually
            tmpTokenFile.deleteOnExit();
            Writer writer = new OutputStreamWriter(new FileOutputStream(tmpTokenFile), StandardCharsets.UTF_8);
            writer.write(authToken.toString());
            writer.close();
            Files.move(tmpTokenFile.toPath(), authTokenCacheFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
            // sets read-write permissions to owner only
            authTokenCacheFile.setReadable(false, false);
            authTokenCacheFile.setReadable(true, true);
            authTokenCacheFile.setWritable(true, true);
        }
        catch (IOException ioe) {
            // if case of any error we just delete the cache, if user-only
            // write permissions are not properly set a security exception
            // is thrown and the file will be deleted.
            authTokenCacheFile.delete();

        }
    }

    /**
     * Return the Hadoop-auth Authenticator to use.
     * <p>
     * It first looks for value of command line option 'auth', if not set it continues to check
     * {@link #AUTHENTICATOR_CLASS_SYS_PROP} Java system property for Authenticator.
     * <p>
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
                throw new OozieClientException(OozieClientException.AUTHENTICATION,
                        "Invalid options provided for auth: " + authOption
                        + ", (" + AuthType.KERBEROS + " or " + AuthType.SIMPLE + " or " + AuthType.BASIC + " expected.)");
            }
            catch (InstantiationException | IllegalAccessException ex) {
                throw new OozieClientException(OozieClientException.AUTHENTICATION,
                        "Could not instantiate Authenticator for option [" + authOption + "], " +
                        ex.getMessage(), ex);
            }

        }

        String className = System.getProperty(AUTHENTICATOR_CLASS_SYS_PROP, KerberosAuthenticator.class.getName());
        if (className != null) {
            try {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                Class<?> klass = (cl != null) ? cl.loadClass(className) :
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
     * null : KerberosAuthenticator
     * SIMPLE : PseudoAuthenticator
     * KERBEROS : KerberosAuthenticator
     *
     * @return the map for classes of Authenticator
     */
    protected Map<String, Class<? extends Authenticator>> getAuthenticators() {
        Map<String, Class<? extends Authenticator>> authClasses = new HashMap<>();
        authClasses.put(AuthType.KERBEROS.toString(), KerberosAuthenticator.class);
        authClasses.put(AuthType.SIMPLE.toString(), PseudoAuthenticator.class);
        authClasses.put(AuthType.BASIC.toString(), BasicAuthenticator.class);
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
