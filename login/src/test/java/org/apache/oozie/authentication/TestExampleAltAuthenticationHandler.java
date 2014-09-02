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

import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.Properties;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.mockito.Mockito;

public class TestExampleAltAuthenticationHandler extends XTestCase {

    private ExampleAltAuthenticationHandler handler;
    private final String redirectUrl = "http://foo:11000/oozie-login/?backurl={0}";

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        new Services().init();
        Services.get().getConf().set("oozie.authentication.ExampleAltAuthenticationHandler.redirect.url", redirectUrl);
        handler = new ExampleAltAuthenticationHandler();
        Properties props = new Properties();
        props.setProperty(ExampleAltAuthenticationHandler.PRINCIPAL, getOoziePrincipal());
        props.setProperty(ExampleAltAuthenticationHandler.KEYTAB, getKeytabFile());
        try {
          handler.init(props);
        } catch (Exception ex) {
          handler = null;
          throw ex;
        }
    }

    @Override
    protected void tearDown() throws Exception {
        if (handler != null) {
            handler.destroy();
            handler = null;
        }
        Services.get().destroy();
        super.tearDown();
    }

    public void testRedirect() throws Exception {
        String oozieBaseUrl = Services.get().getConf().get("oozie.base.url");
        String resolvedRedirectUrl = MessageFormat.format(redirectUrl, URLEncoder.encode(oozieBaseUrl, "ISO-8859-1"));

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        // A User-Agent without "java", "curl", "wget", or "perl" (default) in it is considered to be a browser
        Mockito.when(request.getHeader("User-Agent")).thenReturn("Some Browser");
        // Pretend the request URL is from oozie.base.url
        Mockito.when(request.getRequestURL()).thenReturn(new StringBuffer(oozieBaseUrl));

        // The HttpServletResponse needs to return the encoded redirect url
        Mockito.when(response.encodeRedirectURL(resolvedRedirectUrl)).thenReturn(resolvedRedirectUrl);

        handler.authenticate(request, response);
        Mockito.verify(response).sendRedirect(resolvedRedirectUrl);
    }

    public void testAuthenticateCookie() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        // A User-Agent without "java" in it is considered to be a browser
        Mockito.when(request.getHeader("User-Agent")).thenReturn("Some Browser");

        // We need the request to return the auth cookie
        Cookie[] cookies = {new Cookie("some.other.cookie", "someValue"),
                            new Cookie("oozie.web.login.auth", "someUser")};
        Mockito.when(request.getCookies()).thenReturn(cookies);

        AuthenticationToken token = handler.authenticate(request, response);
        assertEquals("someUser", token.getUserName());
        assertEquals("someUser", token.getName());
        assertEquals("alt-kerberos", token.getType());
    }

    // Some browsers or server implementations will quote cookie values, so test that behavior by repeating testAuthenticateCookie()
    // but with "\"someUser\"" instead of "someUser"
    public void testAuthenticateCookieQuoted() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        // A User-Agent without "java" in it is considered to be a browser
        Mockito.when(request.getHeader("User-Agent")).thenReturn("Some Browser");

        // We need the request to return the auth cookie
        Cookie[] cookies = {new Cookie("some.other.cookie", "someValue"),
                            new Cookie("oozie.web.login.auth", "\"someUser\"")};
        Mockito.when(request.getCookies()).thenReturn(cookies);

        AuthenticationToken token = handler.authenticate(request, response);
        assertEquals("someUser", token.getUserName());
        assertEquals("someUser", token.getName());
        assertEquals("alt-kerberos", token.getType());
    }

    public void testAuthenticateCookieQuotedInvalid() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        // A User-Agent without "java" in it is considered to be a browser
        Mockito.when(request.getHeader("User-Agent")).thenReturn("Some Browser");

        // We need the request to return the auth cookie
        Cookie[] cookies = {new Cookie("some.other.cookie", "someValue"),
                            new Cookie("oozie.web.login.auth", "\"\"")};
        Mockito.when(request.getCookies()).thenReturn(cookies);

        try {
            handler.authenticate(request, response);
        } catch(AuthenticationException ae) {
            assertEquals("Unable to parse authentication cookie", ae.getMessage());
        }
    }
}
