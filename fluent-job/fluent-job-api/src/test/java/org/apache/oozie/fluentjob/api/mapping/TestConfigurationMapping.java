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

package org.apache.oozie.fluentjob.api.mapping;

import com.google.common.collect.ImmutableMap;
import org.apache.oozie.fluentjob.api.generated.workflow.CONFIGURATION;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestConfigurationMapping {
    @Test
    public void testMappingMapToConfiguration() {
        final String key = "key";
        final String value = "value";
        final ImmutableMap<String, String> map = new ImmutableMap.Builder<String, String>().put(key, value).build();

        final CONFIGURATION configuration
                = DozerBeanMapperSingleton.instance().map(map, CONFIGURATION.class);

        final List<CONFIGURATION.Property> properties = configuration.getProperty();
        final CONFIGURATION.Property property = properties.get(0);

        assertEquals(key, property.getName());
        assertEquals(value, property.getValue());
    }
}
