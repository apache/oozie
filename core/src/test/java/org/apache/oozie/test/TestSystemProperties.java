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

package org.apache.oozie.test;

import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorService;

import java.util.HashMap;
import java.util.Map;

public class TestSystemProperties {
    private Map<String, String> sysProps;

    private boolean embeddedHadoop = false;

    private boolean embeddedHadoop2 = false;

    void setupSystemProperties(final String testCaseDir) throws Exception {
        if (System.getProperty("oozielocal.log") == null) {
            setSystemProperty("oozielocal.log", "/tmp/oozielocal.log");
        }
        if (System.getProperty("oozie.test.hadoop.security", "simple").equals("kerberos")) {
            System.setProperty("oozie.service.HadoopAccessorService.kerberos.enabled", "true");
        }
        if (System.getProperty("oozie.test.hadoop.minicluster", "true").equals("true")) {
            embeddedHadoop = true;
            // Second cluster is not necessary without the first one
            if (System.getProperty("oozie.test.hadoop.minicluster2", "false").equals("true")) {
                embeddedHadoop2 = true;
            }
        }

        if (System.getProperty("oozie.test.db.host") == null) {
            System.setProperty("oozie.test.db.host", "localhost");
        }
        setSystemProperty(ConfigurationService.OOZIE_DATA_DIR, testCaseDir);

        setSystemProperty(HadoopAccessorService.SUPPORTED_FILESYSTEMS, "*");
    }

    /**
     * Reset changed system properties to their original values. <p/> Called from {@link XTestCase#tearDown}.
     */
    void resetSystemProperties() {
        if (sysProps != null) {
            for (final Map.Entry<String, String> entry : sysProps.entrySet()) {
                if (entry.getValue() != null) {
                    System.setProperty(entry.getKey(), entry.getValue());
                }
                else {
                    System.getProperties().remove(entry.getKey());
                }
            }
            sysProps.clear();
        }
    }

    /**
     * Set a system property for the duration of the method test case.
     * <p/>
     * After the test method ends the original value is restored.
     *
     * @param name system property name.
     * @param value value to set.
     */
    protected void setSystemProperty(final String name, final String value) {
        if (sysProps == null) {
            sysProps = new HashMap<String, String>();
        }
        if (!sysProps.containsKey(name)) {
            final String currentValue = System.getProperty(name);
            sysProps.put(name, currentValue);
        }
        if (value != null) {
            System.setProperty(name, value);
        }
        else {
            System.getProperties().remove(name);
        }
    }

    boolean isEmbeddedHadoop() {
        return embeddedHadoop;
    }

    boolean isEmbeddedHadoop2() {
        return embeddedHadoop2;
    }
}
