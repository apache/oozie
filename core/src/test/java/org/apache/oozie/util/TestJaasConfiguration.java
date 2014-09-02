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

import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.oozie.test.XTestCase;

public class TestJaasConfiguration extends XTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    // We won't test actually using it to authenticate because that gets messy and may conflict with other tests; but we can test
    // that it otherwise behaves correctly
    public void test() throws Exception {
        String krb5LoginModuleName;
        if (System.getProperty("java.vendor").contains("IBM")) {
            krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
        }
        else {
            krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        }

        JaasConfiguration.clearEntries();
        assertTrue(JaasConfiguration.getEntries().isEmpty());

        JaasConfiguration.addEntry("foo", "foo/localhost", "/some/location/foo");
        assertEquals(1, JaasConfiguration.getEntries().size());
        JaasConfiguration.addEntry("bar", "bar/localhost", "/some/location/bar");
        assertEquals(2, JaasConfiguration.getEntries().size());
        JaasConfiguration.addEntry("zoo", "zoo/localhost", "/some/location/zoo");
        assertEquals(3, JaasConfiguration.getEntries().size());
        checkEntry(krb5LoginModuleName, "foo", "foo/localhost", "/some/location/foo");
        checkEntry(krb5LoginModuleName, "bar", "bar/localhost", "/some/location/bar");
        checkEntry(krb5LoginModuleName, "zoo", "zoo/localhost", "/some/location/zoo");

        JaasConfiguration.removeEntry("bar");
        assertEquals(2, JaasConfiguration.getEntries().size());
        checkEntry(krb5LoginModuleName, "foo", "foo/localhost", "/some/location/foo");
        checkEntry(krb5LoginModuleName, "zoo", "zoo/localhost", "/some/location/zoo");

        JaasConfiguration.clearEntries();
        assertTrue(JaasConfiguration.getEntries().isEmpty());
    }

    private void checkEntry(String loginModuleName, String name, String principal, String keytab) {
        AppConfigurationEntry entry = JaasConfiguration.getEntries().get(name);
        assertEquals(loginModuleName, entry.getLoginModuleName());
        assertEquals(AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, entry.getControlFlag());
        Map<String, ?> options = entry.getOptions();
        assertEquals(keytab, options.get("keyTab"));
        assertEquals(principal, options.get("principal"));
        assertEquals("true", options.get("useKeyTab"));
        assertEquals("true", options.get("storeKey"));
        assertEquals("false", options.get("useTicketCache"));
        assertEquals(5, options.size());
    }
}
