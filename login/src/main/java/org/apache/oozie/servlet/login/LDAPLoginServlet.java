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

import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.servlet.ServletException;

/**
 * This class provides an LDAP example Login Servlet to be used with the ExampleAltAuthenticationHandler.  It provides a login page
 * to the user and checks that the username and password are are able to login to the configured LDAP server and writes the username
 * to a cookie named "oozie.web.login.auth".  Once authenticated, it will send the user to the "backurl".  More information can be
 * found in the README.txt for the Login Server Example.  Note that this implementation is NOT SECURE and should not be used in
 * production.
 */
public class LDAPLoginServlet extends LoginServlet {

    /**
     * Constant for the configuration property that indicates LDAP provider url to use.  Note that this is configured in the web.xml
     * file.
     */
    public static final String LDAP_PROVIDER_URL_KEY = "ldap.provider.url";
    private static final String LDAP_PROVIDER_URL_DEFAULT = "ldap://localhost:389";
    private String ldapProviderUrl;

    /**
     * Constant for the configuration property that indicates LDAP context factory to use.  Note that this is configured in the
     * web.xml file.
     */
    public static final String LDAP_CONTEXT_FACTORY_KEY = "ldap.context.factory";
    private static final String LDAP_CONTEXT_FACTORY_DEFAULT = "com.sun.jndi.ldap.LdapCtxFactory";
    private String ldapContextFactory;

    /**
     * Constant for the configuration property that indicates LDAP security authentication type to use.  Note that this is
     * configured in the web.xml file.
     */
    public static final String LDAP_SECURITY_AUTHENTICATION_KEY = "ldap.security.authentication";
    private static final String LDAP_SECURITY_AUTHENTICATION_DEFAULT = "simple";
    private String ldapSecurityAuthentication;

    @Override
    public void init() throws ServletException {
        super.init();

        ldapProviderUrl = getInitParameter(LDAP_PROVIDER_URL_KEY);
        if (ldapProviderUrl == null) {
            ldapProviderUrl = LDAP_PROVIDER_URL_DEFAULT;
        }

        ldapContextFactory = getInitParameter(LDAP_CONTEXT_FACTORY_KEY);
        if (ldapContextFactory == null) {
            ldapContextFactory = LDAP_CONTEXT_FACTORY_DEFAULT;
        }

        ldapSecurityAuthentication = getInitParameter(LDAP_SECURITY_AUTHENTICATION_KEY);
        if (ldapSecurityAuthentication == null) {
            ldapSecurityAuthentication = LDAP_SECURITY_AUTHENTICATION_DEFAULT;
        }
    }

    /**
     * This method is overridden from LoginServlet to verify the password by attempting to use the username and password to login to
     * the configured LDAP server.
     *
     * @param username The username
     * @param password The password
     * @return true if verified, false if not
     */
    @Override
    protected boolean verifyPassword(String username, String password) {
        boolean result = false;
        try {
            Hashtable<String, String> env = new Hashtable<String, String>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, ldapContextFactory);
            env.put(Context.PROVIDER_URL, ldapProviderUrl);
            env.put(Context.SECURITY_AUTHENTICATION, ldapSecurityAuthentication);
            env.put(Context.SECURITY_PRINCIPAL, username);
            env.put(Context.SECURITY_CREDENTIALS, password);
            DirContext ctx = new InitialDirContext(env);
            if (ctx != null) {
                ctx.close();
                result = true;
            }
        } catch (Exception e) {
            result = false;
        }
        return result;
    }
}
