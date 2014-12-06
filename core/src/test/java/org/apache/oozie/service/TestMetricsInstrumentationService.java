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

package org.apache.oozie.service;

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.MetricsInstrumentation;

public class TestMetricsInstrumentationService extends XTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        services.getConf().set(Services.CONF_SERVICE_EXT_CLASSES, "org.apache.oozie.service.MetricsInstrumentationService");
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testInstrumentation() throws Exception {
        assertNotNull(Services.get().get(InstrumentationService.class));
        assertNotNull(Services.get().get(InstrumentationService.class).get());
        Instrumentation instr = Services.get().get(InstrumentationService.class).get();
        assertTrue(instr instanceof MetricsInstrumentation);
    }

    public void testIsEnabled() {
        assertFalse(InstrumentationService.isEnabled());
        assertTrue(MetricsInstrumentationService.isEnabled());
    }
}
