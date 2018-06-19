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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.Map;

/**
 * A class representing the Oozie subworkflow action.
 * Instances of this class should be built using the builder {@link SubWorkflowActionBuilder}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link SubWorkflowActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SubWorkflowAction extends Node {
    private final String appPath;
    private final boolean propagateConfiguration;
    private final ImmutableMap<String, String> configuration;

    SubWorkflowAction(final Node.ConstructionData constructionData,
                      final String appPath,
                      final boolean propagateConfiguration,
                      final ImmutableMap<String, String> configuration) {
        super(constructionData);

        this.appPath = appPath;
        this.propagateConfiguration = propagateConfiguration;
        this.configuration = configuration;
    }

    /**
     * Returns the path to the application definition (usually workflow.xml) of the subworkflow.
     * @return The path to the application definition (usually workflow.xml) of the subworkflow.
     */
    public String getAppPath() {
        return appPath;
    }

    /**
     * Returns whether the configuration of the main workflow should propagate to the subworkflow.
     * @return {@code true} if the configuration of the main workflow should propagate to the subworkflow;
     *         {@code false} otherwise.
     */
    public boolean isPropagatingConfiguration() {
        return propagateConfiguration;
    }

    /**
     * Returns the value associated with the provided configuration property name.
     * @param property The name of the configuration property for which the value will be returned.
     * @return The value associated with the provided configuration property name.
     */
    public String getConfigProperty(final String property) {
        return configuration.get(property);
    }

    /**
     * Returns an immutable map of the configuration key-value pairs stored in this {@link MapReduceAction} object.
     * @return An immutable map of the configuration key-value pairs stored in this {@link MapReduceAction} object.
     */
    public Map<String, String> getConfiguration() {
        return configuration;
    }
}
