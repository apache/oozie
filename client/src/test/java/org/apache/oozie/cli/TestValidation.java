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
package org.apache.oozie.cli;

import junit.framework.TestCase;

import java.net.URL;
import java.net.URI;
import java.util.concurrent.TimeoutException;
import java.io.File;

public class TestValidation extends TestCase {

    private String getPath(String resource) throws Exception {
        URL url = Thread.currentThread().getContextClassLoader().getResource(resource);
        URI uri = url.toURI();
        File file = new File(uri.getPath());
        return file.getAbsolutePath();
    }

    public void testValid() throws Exception {
        String[] args = new String[]{"validate", getPath("valid.xml")};
        assertEquals(0, new OozieCLI().run(args));
    }

    public void testInvalid() throws Exception {
        String[] args = new String[]{"validate", getPath("invalid.xml")};
        assertEquals(-1, new OozieCLI().run(args));
    }

    // Test for Validation of workflow definition against pattern defined in schema to complete within 3 seconds
    public void testTimeout() throws Exception {
        final String[] args = new String[] { "validate", getPath("timeout.xml") };
        Thread testThread = new Thread() {
            public void run() {
                assertEquals(-1, new OozieCLI().run(args));
            }
        };
        testThread.start();
        Thread.sleep(3000);
        // Timeout if validation takes more than 3 seconds
        testThread.interrupt();

        if (testThread.isInterrupted()) {
            throw new TimeoutException("The pattern validation took too long to complete");
        }
    }

}
