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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;

public class TestConfigUtils extends XTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    @Override
    protected void tearDown() throws Exception {
        if (Services.get() != null) {
            Services.get().destroy();
        }
        super.tearDown();
    }

    public void testGetOozieURL() throws Exception {
        // Normally these are set by a shell script, but not when run from unit tests, so just put some standard values here
        Services.get().getConf().set("oozie.http.hostname", "localhost");
        Services.get().getConf().set("oozie.http.port", "11000");
        Services.get().getConf().set("oozie.https.port", "11443");

        assertEquals("http://localhost:11000/oozie", ConfigUtils.getOozieURL(false));
        assertEquals("https://localhost:11443/oozie", ConfigUtils.getOozieURL(true));
    }

    public void testCheckAndSetNonConflictingUserNamesNoChange() throws Exception {
        checkAndSetNonConflictingNoChange(OozieClient.USER_NAME);
        checkAndSetNonConflictingNoChange(MRJobConfig.USER_NAME);
    }

    protected void checkAndSetNonConflictingNoChange(final String key) throws Exception {
        final Configuration base = new Configuration();
        base.set(key, getTestUser());

        ConfigurationService.setBoolean("oozie.configuration.check-and-set." + key, false);

        ConfigUtils.checkAndSetDisallowedProperties(base, getTestUser(), new Exception(), false);
    }

    public void testCheckAndSetConflictingUserNameSets() throws Exception {
        checkAndSetConflictingSets(OozieClient.USER_NAME);
        checkAndSetConflictingSets(MRJobConfig.USER_NAME);
    }

    protected void checkAndSetConflictingSets(final String key) throws Exception {
        final Configuration base = new Configuration();
        base.set(key, getTestUser());

        ConfigurationService.setBoolean("oozie.configuration.check-and-set." + OozieClient.USER_NAME, true);
        ConfigurationService.setBoolean("oozie.configuration.check-and-set." + MRJobConfig.USER_NAME, true);

        ConfigUtils.checkAndSetDisallowedProperties(base, getTestUser2(), new Exception(), false);

        assertEquals("user.name should be preserved as no write will be performed",
                getTestUser(),
                base.get(key));

        ConfigUtils.checkAndSetDisallowedProperties(base, getTestUser2(), new Exception(), true);

        assertEquals("user.name should be set as one write operation will be performed",
                getTestUser2(),
                base.get(key));
    }

    public void testCheckAndSetConflictingUserNameThrows() {
        checkAndSetConflictingThrows(OozieClient.USER_NAME);
        checkAndSetConflictingThrows(MRJobConfig.USER_NAME);
    }

    protected void checkAndSetConflictingThrows(final String key) {
        final Configuration base = new Configuration();
        base.set(key, getTestUser());

        ConfigurationService.setBoolean("oozie.configuration.check-and-set." + key, false);

        try {
            ConfigUtils.checkAndSetDisallowedProperties(base,
                    getTestUser2(),
                    new CommandException(ErrorCode.E1303, "test error", "test attribute"), false);
            fail("CommandException should have been thrown");
        }
        catch (final CommandException e) {
            assertEquals("ErrorCode mismatch", ErrorCode.E1303, e.getErrorCode());
            assertEquals("message mismatch",
                    "E1303: Invalid bundle application attributes [test error], test attribute",
                    e.getMessage());
        }
    }

    public void testCheckAndSetConflictingUserNamesSetsAndThrows() {
        final Configuration base = new Configuration();
        base.set(OozieClient.USER_NAME, getTestUser());
        base.set(MRJobConfig.USER_NAME, getTestUser());

        ConfigurationService.setBoolean("oozie.configuration.check-and-set." + OozieClient.USER_NAME, true);
        ConfigurationService.setBoolean("oozie.configuration.check-and-set." + MRJobConfig.USER_NAME, false);

        try {
            ConfigUtils.checkAndSetDisallowedProperties(base, getTestUser2(), new Exception("test message"), false);
            fail("Exception should have been thrown");
        }
        catch (final Exception e) {
            assertTrue("message mismatch", e.getMessage().contains("test message"));
        }

        assertEquals("user.name should be preserved as no write will be performed",
                getTestUser(),
                base.get(OozieClient.USER_NAME));

        try {
            ConfigUtils.checkAndSetDisallowedProperties(base, getTestUser2(), new Exception("test message"), true);
        }
        catch (final Exception e) {
            fail("Exception should not have been thrown");
        }

        assertEquals("user.name should be set as one write operation will be performed",
                getTestUser2(),
                base.get(OozieClient.USER_NAME));

        assertEquals("mapreduce.job.user.name should be set implicitly by Configuration#set(user.name)",
                getTestUser2(),
                base.get(MRJobConfig.USER_NAME));
    }
}
