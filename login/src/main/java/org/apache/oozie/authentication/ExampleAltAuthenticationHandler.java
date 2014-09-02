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

package org.apache.oozie.authentication;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
// TODO: Switch to subclassing hadoop's AltKerberosAuthenticationHandler when it becomes available in a hadoop release
//import org.apache.hadoop.security.authentication.server.AltKerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.oozie.service.Services;

/**
 * This class provides an implementation of the {@link AltKerberosAuthenticationHandler} as a simple example.  It is meant to be
 * used with the Login Server Example.  The alternate authentication offered by this class is to check for a cookie named
 * "oozie.web.login.auth" and use its value as the username.  More information can be found in the README.txt for the Login Server
 * Example.  Note that this implementation is NOT SECURE and should not be used in production.
 */
public class ExampleAltAuthenticationHandler extends AltKerberosAuthenticationHandler {

    /**
     * Constant for the configuration property that indicates the redirect URL to send unauthenticated users to the Login Server.
     * It can include {0}, which will be replaced by the Oozie web console URL.
     */
    private static final String REDIRECT_URL = "oozie.authentication.ExampleAltAuthenticationHandler.redirect.url";
    private static final String REDIRECT_URL_DEFAULT = "http://localhost:11000/oozie-login/?backurl={0}";

    private String redirectURL;

    @Override
    public void init(Properties config) throws ServletException {
        super.init(config);

        Configuration conf = Services.get().getConf();
        redirectURL = conf.get(REDIRECT_URL, REDIRECT_URL_DEFAULT);
    }

    /**
     * Implementation of the custom authentication.  It looks for the "oozie.web.login.auth" cookie and if it exists, returns an
     * AuthenticationToken with the cookie's value as the username.  Otherwise, it will redirect the user to the login server via
     * the REDIRECT_URL.
     *
     * @param request the HTTP client request.
     * @param response the HTTP client response.
     * @return an authentication token if the request is authorized, or null
     * @throws IOException thrown if an IO error occurs
     * @throws AuthenticationException thrown if an authentication error occurs
     */
    @Override
    public AuthenticationToken alternateAuthenticate(HttpServletRequest request, HttpServletResponse response)
            throws IOException, AuthenticationException {
        AuthenticationToken token = null;
        Cookie[] cookies = request.getCookies();
        Cookie authCookie = verifyAndExtractAltAuth(cookies);
        String altAuthUserName = getAltAuthUserName(authCookie);
        // Authenticated
        if (altAuthUserName != null) {
            token = new AuthenticationToken(altAuthUserName, altAuthUserName, getType());
        }
        // Not Authenticated
        else {
            StringBuffer sb = request.getRequestURL();
            if (request.getQueryString() != null) {
                sb.append("?").append(request.getQueryString());
            }
            String url = MessageFormat.format(redirectURL, URLEncoder.encode(sb.toString(), "ISO-8859-1"));
            url = response.encodeRedirectURL(url);
            response.sendRedirect(url);
        }
        return token;
    }

    /**
     * Verifies and extracts the "oozie.web.login.auth" Cookie from the passed in cookies.  Note that this implementation doesn't
     * actually do any verification, but a subclass can override it to do so.
     *
     * @param cookies The cookies from a request.
     * @return The "oozie.web.login.auth" cookie or null
     */
    protected Cookie verifyAndExtractAltAuth(Cookie[] cookies) {
        if (cookies == null) {
            return null;
        }
        for (Cookie cookie : cookies) {
            if (cookie.getName().equals("oozie.web.login.auth")) {
                // Here the cookie should be verified for integrity/authenticity from the login service
                return cookie;
            }
        }
        return null;
    }

    /**
     * Returns the username from the "oozie.web.login.auth" cookie.
     *
     * @param authCookie The "oozie.web.login.auth" cookie
     * @return The username from the cookie or null if the cookie is null
     * @throws UnsupportedEncodingException thrown if there's a problem decoding the cookie value
     * @throws AuthenticationException thrown if the cookie value is only two quotes ""
     */
    protected String getAltAuthUserName(Cookie authCookie) throws UnsupportedEncodingException, AuthenticationException {
        if (authCookie == null) {
            return null;
        }
        String username = authCookie.getValue();
        if (username.startsWith("\"") && username.endsWith("\"")) {
            if (username.length() == 2) {
                throw new AuthenticationException("Unable to parse authentication cookie");
            }
            username = username.substring(1, username.length() - 1);
        }
        return URLDecoder.decode(username, "UTF-8");
    }
}
