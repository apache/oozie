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
}
