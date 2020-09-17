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

package org.apache.oozie.server.guice;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.server.EmbeddedOozieServer;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestConstraintSecurityHandlerProvider {
    @Mock Services mockServices;
    @Mock ConfigurationService mockCfgService;
    @Mock Configuration mockConfig;

    @Test
    public void constraintHandlersCanBeSet() {
        Configuration config = new Configuration(false);
        config.set("oozie.base.url", "https://localhost:11443/oozie");
        when(mockCfgService.getConf()).thenReturn(config);
        when(mockServices.get(ConfigurationService.class)).thenReturn(mockCfgService);

        ConstraintSecurityHandlerProvider constraintSecurityHandlerProvider = new ConstraintSecurityHandlerProvider(
                mockServices);
        ConstraintSecurityHandler actConstraintSecurityHandler = constraintSecurityHandlerProvider.get();
        List<ConstraintMapping> actConstraintMappings = actConstraintSecurityHandler.getConstraintMappings();
        assertEquals(actConstraintMappings.size(), 2);

        List<String> actPathSpecs = new ArrayList<>();
        for (ConstraintMapping sm : actConstraintMappings) {
            actPathSpecs.add(sm.getPathSpec());
        }

        assertTrue(actPathSpecs.contains(String.format("%s/callback/*", EmbeddedOozieServer.getContextPath(config))));
        assertTrue(actPathSpecs.contains("/*"));
    }
}
