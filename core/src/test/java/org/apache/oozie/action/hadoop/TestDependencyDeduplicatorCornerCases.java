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
package org.apache.oozie.action.hadoop;

import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDependencyDeduplicatorCornerCases {
    private static final Configuration DEFAULT_CONF = new Configuration();
    private static final String KEY = "key";
    static {
        DEFAULT_CONF.set(KEY, "some_value");
    }
    private DependencyDeduplicator deduplicator;
    private Configuration conf;
    private String key;
    private String assertMessage;

    public TestDependencyDeduplicatorCornerCases(final String testName, final Configuration conf,
                                                 final String key, final String assertMessage) {
        this.conf = conf;
        this.key = key;
        this.assertMessage = assertMessage;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection testCases() {
        return Arrays.asList(new Object[][] {
                {"Test with null key", DEFAULT_CONF, null, "[null] value as a key shall be handled."},
                {"Test with null key and conf", null, null, "[null] value as a key or conf shall be handled."},
                {"Test with null conf but real key", null, KEY, "[null] value as conf shall be handled when key is not null, too."},
                {"Test with invalid key", DEFAULT_CONF, "nonexistentkey", "[null] value for a key shall be handled."}
        });
    }

    @Before
    public void init() {
        deduplicator = new DependencyDeduplicator();
    }

    @Test
    public void testCornerCase() {
        try {
            deduplicator.deduplicate(conf, key);
        } catch (NullPointerException npe) {
            Assert.fail(assertMessage);
        } catch (Exception e) {
            Assert.fail("No exception should be thrown.");
        }
    }
}
