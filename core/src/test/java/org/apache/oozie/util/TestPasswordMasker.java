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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPasswordMasker {

    @Test
    public void testWhenJavaSystemPropertiesAreAskedPasswordsAppearMasked() throws Exception {
        Map<String, String> masked = new PasswordMasker().mask(jsonToMap("/instrumentation-system-properties.json"));

        assertPasswordValueIsMasked(masked, "javax.net.ssl.trustStorePassword");
        assertPasswordValueIsMasked(masked, "oozie.https.keystore.pass");
    }

    @Test
    public void testWhenOSEnvIsAskedPasswordsAppearMasked() throws Exception {
        Map<String, String> masked = new PasswordMasker().mask(jsonToMap("/instrumentation-os-env.json"));

        assertPasswordValueIsMasked(masked, "HADOOP_CREDSTORE_PASSWORD");
        assertPasswordValueIsMasked(masked, "OOZIE_HTTPS_KEYSTORE_PASSWORD");
        assertPasswordValueIsMasked(masked, "OOZIE_HTTPS_TRUSTSTORE_PASSWORD");

        assertPasswordValueFragmentIsMasked(masked, "CATALINA_OPTS", "-Doozie.https.keystore.pass=");
        assertPasswordValueFragmentIsMasked(masked, "CATALINA_OPTS", "-Djavax.net.ssl.trustStorePassword=");

        assertValueFragmentIsPresent(masked, "CATALINA_OPTS", "-Xmx1024m");
        assertValueFragmentIsPresent(masked, "CATALINA_OPTS", "-Doozie.https.keystore.file=/Users/forsage/.keystore");
        assertValueFragmentIsPresent(masked, "CATALINA_OPTS", "-Djava.library.path=");
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> jsonToMap(String jsonPath) throws IOException {
        return new ObjectMapper().readValue(getClass().getResourceAsStream(jsonPath), HashMap.class);
    }

    private void assertPasswordValueIsMasked(Map<String, String> mapContainingMaskedPassword, String passwordKey) {
        assertEquals(String.format("Value of key '%s' should be masked.", passwordKey),
                "*****",
                mapContainingMaskedPassword.get(passwordKey));
    }

    private void assertPasswordValueFragmentIsMasked(Map<String, String> mapContainingMaskedPassword, String passwordKey,
                                                     String passwordFragmentKey) {
        assertEquals(
                String.format("Value fragment of password key '%s' and password fragment key '%s' should be masked.",
                        passwordKey,
                        passwordFragmentKey),
                "*****",
                getFragmentValue(mapContainingMaskedPassword.get(passwordKey), passwordFragmentKey));
    }

    private String getFragmentValue(String base, String fragmentKey) {
        for (String fragment : base.split(" ")) {
            if (fragment.startsWith(fragmentKey)) {
                return fragment.substring(fragmentKey.length());
            }
        }

        return null;
    }

    private void assertValueFragmentIsPresent(Map<String, String> masked, String key, String valueFragment) {
        assertTrue(String.format("For key '%s' value fragment '%s' should be present.", key, valueFragment),
                masked.get(key).contains(valueFragment));
    }
}
