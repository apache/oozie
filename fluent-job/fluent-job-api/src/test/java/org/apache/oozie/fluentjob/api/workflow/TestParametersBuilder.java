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

package org.apache.oozie.fluentjob.api.workflow;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestParametersBuilder {
    private ParametersBuilder builder;

    @Before
    public void setUp() {
        this.builder = ParametersBuilder.create();
    }

    @Test
    public void testWithoutDescription() {
        final Parameters parameters = builder
                .withParameter("name1", "value1")
                .withParameter("name2", "value2")
                .build();

        assertEquals("name1", parameters.getParameters().get(0).getName());
        assertEquals("value1", parameters.getParameters().get(0).getValue());
        assertNull(parameters.getParameters().get(0).getDescription());
        assertEquals("name2", parameters.getParameters().get(1).getName());
        assertEquals("value2", parameters.getParameters().get(1).getValue());
        assertNull(parameters.getParameters().get(1).getDescription());
    }

    @Test
    public void testWithDescription() {
        final Parameters parameters = builder
                .withParameter("name1", "value1", "description1")
                .withParameter("name2", "value2", "description2")
                .build();

        assertEquals("name1", parameters.getParameters().get(0).getName());
        assertEquals("value1", parameters.getParameters().get(0).getValue());
        assertEquals("description1", parameters.getParameters().get(0).getDescription());
        assertEquals("name2", parameters.getParameters().get(1).getName());
        assertEquals("value2", parameters.getParameters().get(1).getValue());
        assertEquals("description2", parameters.getParameters().get(1).getDescription());
    }

    @Test
    public void testCreateFromExisting() {
        final Parameters existing = builder
                .withParameter("name1", "value1")
                .withParameter("name2", "value2")
                .build();

        final Parameters fromExisting = ParametersBuilder.createFromExisting(existing)
                .withParameter("name3", "value3")
                .build();

        assertEquals("value1", fromExisting.getParameters().get(0).getValue());
        assertEquals("value2", fromExisting.getParameters().get(1).getValue());
        assertEquals("value3", fromExisting.getParameters().get(2).getValue());
    }
}