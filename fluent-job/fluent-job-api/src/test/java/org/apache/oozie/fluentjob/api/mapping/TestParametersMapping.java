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

import org.apache.oozie.fluentjob.api.generated.workflow.PARAMETERS;
import org.apache.oozie.fluentjob.api.workflow.Parameters;
import org.apache.oozie.fluentjob.api.workflow.ParametersBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestParametersMapping {

    @Test
    public void testMappingParameters() {
        final Parameters source = ParametersBuilder.create()
                .withParameter("name1", "value1")
                .withParameter("name2", "value2", "description2")
                .build();

        final PARAMETERS destination = DozerBeanMapperSingleton.instance().map(source, PARAMETERS.class);

        assertEquals("name1", destination.getProperty().get(0).getName());
        assertEquals("value1", destination.getProperty().get(0).getValue());
        assertNull(destination.getProperty().get(0).getDescription());
        assertEquals("name2", destination.getProperty().get(1).getName());
        assertEquals("value2", destination.getProperty().get(1).getValue());
        assertEquals("description2", destination.getProperty().get(1).getDescription());
    }
}
