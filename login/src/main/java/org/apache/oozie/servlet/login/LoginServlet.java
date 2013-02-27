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
package org.apache.oozie.servlet.login;

import java.io.*;
import java.net.URLEncoder;
import java.text.MessageFormat;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This class provides a basic example Login Servlet to be used with the ExampleAltAuthenticationHandler.  It provides a login page
 * to the user and simply checks that the username and password are equal (e.g. user=foo pass=foo) and writes the username to a
 * cookie named "oozie.web.login.auth".  Once authenticated, it will send the user to the "backurl".  More information can be found
 * in the README.txt for the Login Server Example.  Note that this implementation is NOT SECURE and should not be used in
 * production.
 */
public class LoginServlet extends HttpServlet {

    /**
     * Constant for the configuration property that indicates the login page html to use.  The file needs to be located in the
     * login/src/main/resources/ directory and should contain {0} for where an error message can go, {1} for where the username
     * included with a GET request will go, and {2} for where the "backurl" goes.  Note that this is configured in the web.xml file.
     */
    public static final String LOGIN_PAGE_TEMPLATE_KEY = "login.page.template";
    private static final String LOGIN_PAGE_TEMPLATE_DEFAULT = "login-page-template.html";
    private String loginPageTemplate;

    /**
     * Constant for the configuration property that indicates the expiration time (or max age) of the "oozie.web.login.auth" cookie.
     * It is given in seconds.  A positive value indicates that the cookie will expire after that many seconds have passed; make
     * sure this value is high enough to allow the user to be forwarded to the backurl before the cookie expires.  A negative value
     * indicates that the cookie will be deleted when the browser exits.
     */
    public static final String LOGIN_AUTH_COOKIE_EXPIRE_TIME = "login.auth.cookie.expire.time";
    private static final int LOGIN_AUTH_COOKIE_EXPIRE_TIME_DEFAULT = 180;   // 3 minutes
    private int loginAuthCookieExpireTime;

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String BACKURL = "backurl";

    @Override
    public void init() throws ServletException {
        // Read in the login page html
        String loginPageTemplateName = getInitParameter(LOGIN_PAGE_TEMPLATE_KEY);
        if (loginPageTemplateName == null) {
            loginPageTemplateName = LOGIN_PAGE_TEMPLATE_DEFAULT;
        }
        InputStream is = getClass().getClassLoader().getResourceAsStream(loginPageTemplateName);
        if (is == null) {
            throw new ServletException("Could not find resource [" + loginPageTemplateName + "]");
        }
        try {
            StringBuilder sb = new StringBuilder();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line = br.readLine();
            while (line != null) {
                sb.append(line).append("\n");
                line = br.readLine();
            }
            br.close();
            loginPageTemplate = sb.toString();
        } catch (IOException ex) {
            throw new ServletException("Could not read resource [" + loginPageTemplateName + "]");
        }

        // Read in the cookie expiration time
        String cookieExpireTime = getInitParameter(LOGIN_AUTH_COOKIE_EXPIRE_TIME);
        if (cookieExpireTime == null) {
            loginAuthCookieExpireTime = LOGIN_AUTH_COOKIE_EXPIRE_TIME_DEFAULT;
        }
        else {
            try {
                loginAuthCookieExpireTime = Integer.parseInt(cookieExpireTime);
            }
            catch (NumberFormatException nfe) {
                throw new ServletException(LOGIN_AUTH_COOKIE_EXPIRE_TIME + " must be a valid integer", nfe);
            }
        }
    }

    protected void renderLoginPage(String message, String username, String backUrl, HttpServletResponse resp)
            throws ServletException, IOException {
        resp.setContentType("text/html");
        Writer writer = resp.getWriter();
        writer.write(MessageFormat.format(loginPageTemplate, message, username, backUrl));
        writer.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // Check for the optional username parameter
        String username = req.getParameter(USERNAME);
        if (username == null) {
            username = "";
        }
        // Check for the required backurl parameter
        String backUrl = req.getParameter(BACKURL);
        if (backUrl == null || backUrl.trim().isEmpty()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "missing or invalid '" + BACKURL + "' parameter");
        }
        else {
            renderLoginPage("", username, backUrl, resp);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String backUrl = req.getParameter(BACKURL);
        // Check for the required backurl parameter
        if (backUrl == null || backUrl.trim().isEmpty()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "missing or invalid '" + BACKURL + "' parameter");
        } else {
            // Check for the requried username and password parameters
            String username = req.getParameter(USERNAME);
            String password = req.getParameter(PASSWORD);
            if (username == null || username.trim().isEmpty()) {
                renderLoginPage("<font color=\"red\">Error: Invalid Username or Password</font><br>", "", backUrl, resp);
            }
            else if (password == null || password.trim().isEmpty()) {
                renderLoginPage("<font color=\"red\">Error: Invalid Username or Password</font><br>", username, backUrl, resp);
            }
            // Verify that the username and password are correct
            else if (verifyPassword(username, password)) {
                // If so, write the "oozie.web.login.auth" cookie and redirect back to the backurl
                writeCookie(resp, username);
                resp.sendRedirect(backUrl);
            } else {
                renderLoginPage("<font color=\"red\">Error: Invalid Username or Password</font><br>", username, backUrl, resp);
            }
        }
    }

    /**
     * Verify that the given username and password are correct.  In this implementation, they are correct when they are equal, but
     * a subclass can override this to provide a more complex/secure mechanism.
     *
     * @param username The username
     * @param password The password
     * @return true if verified, false if not
     */
    protected boolean verifyPassword(String username, String password) {
        return (username.equals(password));
    }

    /**
     * Write the "oozie.web.login.auth" cookie containing the username.  A subclass can override this to include more information
     * into the cookie; though this will likely break compatibility with the ExampleAltAuthenticationHandler, so it would have to
     * be extended as well.  It is recommended that the cookie value be URL-encoded.
     *
     * @param resp The response
     * @param username The username
     * @throws UnsupportedEncodingException thrown when there is a problem encoding the username as the cookie value
     */
    protected void writeCookie(HttpServletResponse resp, String username) throws UnsupportedEncodingException {
        Cookie cookie = new Cookie("oozie.web.login.auth", URLEncoder.encode(username, "UTF-8"));
        cookie.setPath("/");
        cookie.setMaxAge(loginAuthCookieExpireTime);
        resp.addCookie(cookie);
    }
}
