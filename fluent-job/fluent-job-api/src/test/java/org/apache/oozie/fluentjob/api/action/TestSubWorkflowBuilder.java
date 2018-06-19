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

package org.apache.oozie.fluentjob.api.action;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestSubWorkflowBuilder extends TestNodeBuilderBaseImpl<SubWorkflowAction, SubWorkflowActionBuilder> {
    private static final String MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name";
    private static final String DEFAULT = "default";

    private static final ImmutableMap<String, String> CONFIG_EXAMPLE = getConfigExample();

    private static ImmutableMap<String, String> getConfigExample() {
        final ImmutableMap.Builder<String, String> configExampleBuilder = new ImmutableMap.Builder<>();

        final String[] keys = {"mapred.map.tasks", "mapred.input.dir", "mapred.output.dir"};
        final String[] values = {"1", "${inputDir}", "${outputDir}"};

        for (int i = 0; i < keys.length; ++i) {
            configExampleBuilder.put(keys[i], values[i]);
        }

        return configExampleBuilder.build();
    }

    @Override
    protected SubWorkflowActionBuilder getBuilderInstance() {
        return SubWorkflowActionBuilder.create();
    }

    @Override
    protected SubWorkflowActionBuilder getBuilderInstance(SubWorkflowAction action) {
        return SubWorkflowActionBuilder.createFromExistingAction(action);
    }

    @Test
    public void testWithAppPath() {
        final String appPath = "/path/to/app";

        final SubWorkflowActionBuilder builder = getBuilderInstance();
        builder.withAppPath(appPath);

        final SubWorkflowAction action = builder.build();
        assertEquals(appPath, action.getAppPath());
    }

    @Test
    public void testWithAppPathCalledTwiceThrows() {
        final String appPath1 = "/path/to/app1";
        final String appPath2 = "/path/to/app2";

        final SubWorkflowActionBuilder builder = getBuilderInstance();
        builder.withAppPath(appPath1);

        expectedException.expect(IllegalStateException.class);
        builder.withAppPath(appPath2);
    }

    @Test
    public void testWithPropagatingConfiguration() {
        final SubWorkflowActionBuilder builder = getBuilderInstance();
        builder.withPropagatingConfiguration();

        final SubWorkflowAction action = builder.build();
        assertEquals(true, action.isPropagatingConfiguration());
    }

    @Test
    public void testWithPropagatingConfigurationCalledTwiceThrows() {
        final SubWorkflowActionBuilder builder = getBuilderInstance();
        builder.withPropagatingConfiguration();

        expectedException.expect(IllegalStateException.class);
        builder.withPropagatingConfiguration();
    }

    @Test
    public void testWithoutPropagatingConfiguration() {
        final SubWorkflowActionBuilder builder = getBuilderInstance();
        builder.withPropagatingConfiguration();

        final SubWorkflowAction action = builder.build();

        final SubWorkflowActionBuilder fromExistingBuilder = getBuilderInstance(action);

        fromExistingBuilder.withoutPropagatingConfiguration();

        final SubWorkflowAction modifiedAction = fromExistingBuilder.build();
        assertEquals(false, modifiedAction.isPropagatingConfiguration());
    }

    @Test
    public void testWithoutPropagatingConfigurationCalledTwiceThrows() {
        final SubWorkflowActionBuilder builder = getBuilderInstance();
        builder.withPropagatingConfiguration();

        final SubWorkflowAction action = builder.build();

        final SubWorkflowActionBuilder fromExistingBuilder = getBuilderInstance(action);

        fromExistingBuilder.withoutPropagatingConfiguration();

        expectedException.expect(IllegalStateException.class);
        fromExistingBuilder.withoutPropagatingConfiguration();
    }

    @Test
    public void testConfigPropertyAdded() {
        final SubWorkflowActionBuilder builder = getBuilderInstance();
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);

        final SubWorkflowAction action = builder.build();
        assertEquals(DEFAULT, action.getConfiguration().get(MAPRED_JOB_QUEUE_NAME));
    }

    @Test
    public void testSeveralConfigPropertiesAdded() {
        final SubWorkflowActionBuilder builder = getBuilderInstance();

        for (final Map.Entry<String, String> entry : CONFIG_EXAMPLE.entrySet()) {
            builder.withConfigProperty(entry.getKey(), entry.getValue());
        }

        final SubWorkflowAction action = builder.build();

        for (final Map.Entry<String, String> entry : CONFIG_EXAMPLE.entrySet()) {
            assertEquals(entry.getValue(), action.getConfiguration().get(entry.getKey()));
        }

        assertEquals(CONFIG_EXAMPLE, action.getConfiguration());
    }

    @Test
    public void testSameConfigPropertyAddedTwiceThrows() {
        final SubWorkflowActionBuilder builder = getBuilderInstance();
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);

        expectedException.expect(IllegalStateException.class);
        builder.withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);
    }

    @Test
    public void testFromExistiongSubWorkflowAction() {
        final String appPath = "/path/to/app";

        final SubWorkflowActionBuilder builder = getBuilderInstance();
        builder.withAppPath(appPath)
                .withPropagatingConfiguration()
                .withConfigProperty(MAPRED_JOB_QUEUE_NAME, DEFAULT);

        final SubWorkflowAction action = builder.build();

        final SubWorkflowActionBuilder fromExistingBuilder = getBuilderInstance(action);

        final SubWorkflowAction modifiedAction = fromExistingBuilder.build();
        assertEquals(appPath, modifiedAction.getAppPath());
        assertEquals(true, modifiedAction.isPropagatingConfiguration());

        final Map<String, String> expectedConfiguration = new LinkedHashMap<>();
        expectedConfiguration.put(MAPRED_JOB_QUEUE_NAME, DEFAULT);
        assertEquals(expectedConfiguration, modifiedAction.getConfiguration());
    }
}
