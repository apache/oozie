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

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.servlet.http.HttpServletResponse;
import org.apache.directory.server.core.configuration.MutablePartitionConfiguration;
import org.apache.directory.server.unit.AbstractServerTest;

// LDAP stuff based on https://cwiki.apache.org/DIRxSRVx10/using-apacheds-for-unit-tests.html
// The default admin user for Apache DS is "uid=admin,ou=system" and password is "secret"
public class TestLDAPLoginServlet extends AbstractServerTest {

    // We need to subclass the AbstractServerTest to get the LDAP stuff, so we'll have to do a wrapper to inherit the
    // TestLoginServlet tests instead of subclassing it
    TestLoginServlet tls = new TestLoginServlet() {
        @Override
        protected Class getServletClass() {
            // Make the TestLoginServlet use LDAPLoginServlet instead of LoginServlet
            return LDAPLoginServlet.class;
        }

        @Override
        protected Map<String, String> getInitParameters() {
            // Configure for LDAP tests
            HashMap<String, String> initParams = new HashMap<String, String>();
            initParams.put("ldap.provider.url", "o=test");
            initParams.put("ldap.context.factory", "org.apache.directory.server.jndi.ServerContextFactory");
            return initParams;
        }
    };

    @Override
    public void setUp() throws Exception {
        // Add partition 'test'
        MutablePartitionConfiguration pcfg = new MutablePartitionConfiguration();
        pcfg.setName("test");
        pcfg.setSuffix("o=test");

        // Create some indices
        Set<String> indexedAttrs = new HashSet<String>();
        indexedAttrs.add("objectClass");
        indexedAttrs.add("o");
        pcfg.setIndexedAttributes(indexedAttrs);

        // Create a first entry associated to the partition
        Attributes attrs = new BasicAttributes(true);

        // First, the objectClass attribute
        Attribute attr = new BasicAttribute("objectClass");
        attr.add("top");
        attr.add("organization");
        attrs.put(attr);

        // The the 'Organization' attribute
        attr = new BasicAttribute("o");
        attr.add("test");
        attrs.put(attr);

        // Associate this entry to the partition
        pcfg.setContextEntry(attrs);

        // As we can create more than one partition, we must store
        // each created partition in a Set before initialization
        Set<MutablePartitionConfiguration> pcfgs = new HashSet<MutablePartitionConfiguration>();
        pcfgs.add(pcfg);

        configuration.setContextPartitionConfigurations(pcfgs);

        // Create a working directory
        File workingDirectory = new File("server-work");
        configuration.setWorkingDirectory(workingDirectory);

        // Now, let's call the super class which is responsible for the
        // partitions creation
        super.setUp();

        // setUp the TestLoginServlet
        tls.setUp();
    }

    public void testGetMissingBackurl() throws Exception {
        tls.testGetMissingBackurl();
    }

    public void testGetSuccess() throws Exception {
        tls.testGetSuccess();
    }

    public void testPostMissingBackurl() throws Exception {
        tls.testPostMissingBackurl();
    }

    public void testPostMissingUsernamePassword() throws Exception {
        tls.testPostMissingUsernamePassword();
    }

    public void testPostInvalidUsernamePassword() throws Exception {
        // Valid username, invalid password
        URL url = new URL(tls.container.getServletURL("/")
                + "?backurl=http://foo:11000/oozie&username=uid=admin,ou=system&password=bar");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
        String html = tls.getHTML(conn);
        assertEquals(MessageFormat.format(TestLoginServlet.loginPageTemplate,
                "<font color=\"red\">Error: Invalid Username or Password</font><br>",
                "uid=admin,ou=system", "http://foo:11000/oozie"), html);

        // InValid username, valid password
        url = new URL(tls.container.getServletURL("/")
                + "?backurl=http://foo:11000/oozie&username=foo&password=secret");
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
        html = tls.getHTML(conn);
        assertEquals(MessageFormat.format(TestLoginServlet.loginPageTemplate,
                "<font color=\"red\">Error: Invalid Username or Password</font><br>", "foo", "http://foo:11000/oozie"), html);
    }

    public void testPostSuccess() throws Exception {
        // Now that its actually going to work successfully, the backurl needs to go somewhere real; about:blank provides a
        // convinient location that doesn't require internet access or another servlet running locally
        URL url = new URL(tls.container.getServletURL("/") + "?backurl=about:blank&username=uid=admin,ou=system&password=secret");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        assertEquals(HttpServletResponse.SC_FOUND, conn.getResponseCode());
        String cookies = tls.getCookies(conn);
        String username = tls.getUsernameFromCookies(cookies);
        assertEquals("uid=admin,ou=system", username);
    }

    @Override
    public void tearDown() throws Exception {
        // tear down the TestLoginServlet
        tls.tearDown();

        super.tearDown();
    }
}
