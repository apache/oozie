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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPasswordMasker {
    private PasswordMasker passwordMasker;

    @Before
    public void setup() {
        passwordMasker = new PasswordMasker();
    }

    @Test
    public void testWhenJavaSystemPropertiesAreAskedPasswordsAppearMasked() throws Exception {
        Map<String, String> masked = passwordMasker.mask(jsonToMap("/instrumentation-system-properties.json"));

        assertPasswordValueIsMasked(masked, "javax.net.ssl.trustStorePassword");
        assertPasswordValueIsMasked(masked, "oozie.https.keystore.pass");
    }

    @Test
    public void testWhenOSEnvIsAskedPasswordsAppearMasked() throws Exception {
        Map<String, String> masked = passwordMasker.mask(jsonToMap("/instrumentation-os-env.json"));

        assertPasswordValueIsMasked(masked, "HADOOP_CREDSTORE_PASSWORD");
        assertPasswordValueIsMasked(masked, "OOZIE_HTTPS_KEYSTORE_PASSWORD");
        assertPasswordValueIsMasked(masked, "OOZIE_HTTPS_TRUSTSTORE_PASSWORD");

        assertPasswordValueFragmentIsMasked(masked, "CATALINA_OPTS", "-Doozie.https.keystore.pass=");
        assertPasswordValueFragmentIsMasked(masked, "CATALINA_OPTS", "-Djavax.net.ssl.trustStorePassword=");

        assertValueFragmentIsPresent(masked, "CATALINA_OPTS", "-Xmx1024m");
        assertValueFragmentIsPresent(masked, "CATALINA_OPTS", "-Doozie.https.keystore.file=/Users/forsage/.keystore");
        assertValueFragmentIsPresent(masked, "CATALINA_OPTS", "-Djava.library.path=");
    }

    @Test
    public void testMaskNothing() {
        assertEquals("abcd", passwordMasker.maskPasswordsIfNecessary("abcd"));
        assertEquals("abcd abcd", passwordMasker.maskPasswordsIfNecessary("abcd abcd"));
        assertEquals("-Djava.net.pasX=pwd1", passwordMasker.maskPasswordsIfNecessary("-Djava.net.pasX=pwd1"));
    }

    @Test
    public void testMaskJavaSystemProp() {
        assertEquals("-Djava.sysprop.password=*****", passwordMasker.maskPasswordsIfNecessary("-Djava.sysprop.password=pwd123"));
    }

    @Test
    public void testMaskJavaSystemPropWithWhiteSpaces() {
        assertEquals("  -Djava.sysprop.password=*****  ",
                passwordMasker.maskPasswordsIfNecessary("  -Djava.sysprop.password=pwd123  "));
    }

    @Test
    public void testMaskTwoJavaSystemProps() {
        assertEquals("-Djava.sysprop.password=***** -Djava.another.password=*****",
                passwordMasker.maskPasswordsIfNecessary("-Djava.sysprop.password=pwd123 -Djava.another.password=pwd456"));
    }

    @Test
    public void testMaskEnvironmentVariable() {
        assertEquals("DUMMY_PASSWORD=*****", passwordMasker.maskPasswordsIfNecessary("DUMMY_PASSWORD=dummy"));
    }

    @Test
    public void testMaskTwoEnvironmentVariables() {
        assertEquals("DUMMY_PASSWORD=*****:ANOTHER_PASSWORD=*****",
                passwordMasker.maskPasswordsIfNecessary("DUMMY_PASSWORD=dummy:ANOTHER_PASSWORD=pwd123"));
    }

    @Test
    public void testMaskRandomMatchingStuff() {
        assertEquals("aa -Djava.sysprop.password=***** bb DUMMY_PASSWORD=***** cc",
                passwordMasker.maskPasswordsIfNecessary("aa -Djava.sysprop.password=1234 bb DUMMY_PASSWORD=dummy cc"));
    }

    @Test
    public void testMaskNullArgument() {
        assertEquals(null, passwordMasker.maskPasswordsIfNecessary(null));
    }

    @Test
    public void testMaskEmptyArgument() {
        assertEquals("", passwordMasker.maskPasswordsIfNecessary(""));
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
