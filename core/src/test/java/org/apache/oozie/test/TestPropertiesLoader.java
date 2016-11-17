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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class TestPropertiesLoader {

    public File loadTestPropertiesOrThrow() {
        try {
            final File oozieSrcDir = findOozieSrcDir();

            final String testPropsFile = System.getProperty(XTestCase.OOZIE_TEST_PROPERTIES, "test.properties");
            final File file = new File(testPropsFile).isAbsolute()
                    ? new File(testPropsFile) : new File(oozieSrcDir, testPropsFile);
            if (file.exists()) {
                loadTestProperties(file);
            }
            else {
                checkTestPropertiesAndError();
            }

            return oozieSrcDir;
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private File findOozieSrcDir() {
        File oozieSrcDir = new File("core").getAbsoluteFile();

        if (!oozieSrcDir.exists()) {
            oozieSrcDir = oozieSrcDir.getParentFile().getParentFile();
            oozieSrcDir = new File(oozieSrcDir, "core");
        }
        if (!oozieSrcDir.exists()) {
            oozieSrcDir = oozieSrcDir.getParentFile().getParentFile();
            oozieSrcDir = new File(oozieSrcDir, "core");
        }
        if (!oozieSrcDir.exists()) {
            // We're probably being run from outside of Oozie (e.g. MiniOozie), so just use a dummy location here.
            // Anything that uses this location should have a fallback anyway.
            oozieSrcDir = new File(".");
        }
        else {
            oozieSrcDir = oozieSrcDir.getParentFile();
        }

        return oozieSrcDir;
    }

    private void loadTestProperties(final File file) throws IOException {
        System.out.println();
        System.out.println("*********************************************************************************");
        System.out.println("Loading test system properties from: " + file.getAbsolutePath());
        System.out.println();
        final Properties props = new Properties();
        props.load(new FileReader(file));
        for (final Map.Entry entry : props.entrySet()) {
            if (!System.getProperties().containsKey(entry.getKey())) {
                System.setProperty((String) entry.getKey(), (String) entry.getValue());
                System.out.println(entry.getKey() + " = " + entry.getValue());
            }
            else {
                System.out.println(entry.getKey() + " IGNORED, using command line value = " +
                        System.getProperty((String) entry.getKey()));
            }
        }
        System.out.println("*********************************************************************************");
        System.out.println();
    }

    private void checkTestPropertiesAndError() {
        if (System.getProperty(XTestCase.OOZIE_TEST_PROPERTIES) != null) {
            System.err.println();
            System.err.println("ERROR: Specified test file does not exist: " +
                    System.getProperty(XTestCase.OOZIE_TEST_PROPERTIES));
            System.err.println();
            System.exit(-1);
        }
    }
}
