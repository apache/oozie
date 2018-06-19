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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.oozie.fluentjob.api.ModifyOnce;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A builder class for {@link SubWorkflowAction}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link SubWorkflowActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SubWorkflowActionBuilder
        extends NodeBuilderBaseImpl<SubWorkflowActionBuilder> implements Builder<SubWorkflowAction> {
    private final ModifyOnce<String> appPath;
    private final ModifyOnce<Boolean> propagateConfiguration;
    private final Map<String, ModifyOnce<String>> configuration;

    /**
     * Creates and returns an empty builder.
     * @return An empty builder.
     */
    public static SubWorkflowActionBuilder create() {
        final ModifyOnce<String> appPath = new ModifyOnce<>();
        final ModifyOnce<Boolean> propagateConfiguration = new ModifyOnce<>(false);
        final Map<String, ModifyOnce<String>> configuration = new LinkedHashMap<>();

        return new SubWorkflowActionBuilder(null, appPath, propagateConfiguration, configuration);
    }

    /**
     * Create and return a new {@link SubWorkflowActionBuilder} that is based on an already built
     * {@link SubWorkflowAction} object. The properties of the builder will initially be the same as those of the
     * provided {@link SubWorkflowAction} object, but it is possible to modify them once.
     * @param action The {@link SubWorkflowAction} object on which this {@link SubWorkflowActionBuilder} will be based.
     * @return A new {@link SubWorkflowActionBuilder} that is based on a previously built {@link SubWorkflowAction} object.
     */
    public static SubWorkflowActionBuilder createFromExistingAction(final SubWorkflowAction action) {
        final ModifyOnce<String> appPath = new ModifyOnce<>(action.getAppPath());
        final ModifyOnce<Boolean> propagateConfiguration = new ModifyOnce<>(action.isPropagatingConfiguration());
        final Map<String, ModifyOnce<String>> configuration =
                ActionAttributesBuilder.convertToModifyOnceMap(action.getConfiguration());

        return new SubWorkflowActionBuilder(action, appPath, propagateConfiguration, configuration);
    }

    SubWorkflowActionBuilder(final SubWorkflowAction action,
                             final ModifyOnce<String> appPath,
                             final ModifyOnce<Boolean> propagateConfiguration,
                             final Map<String, ModifyOnce<String>> configuration) {
        super(action);

        this.appPath = appPath;
        this.propagateConfiguration = propagateConfiguration;
        this.configuration = configuration;
    }

    /**
     * Registers the path to the application definition (usually workflow.xml) of the subworkflow.
     * @param appPath HDFS application path
     * @return This builder.
     */
    public SubWorkflowActionBuilder withAppPath(final String appPath) {
        this.appPath.set(appPath);
        return this;
    }

    /**
     * Registers that the configuration of the main workflow should propagate to the subworkflow.
     * @return This builder.
     */
    public SubWorkflowActionBuilder withPropagatingConfiguration() {
        this.propagateConfiguration.set(true);
        return this;
    }

    /**
     * Registers that the configuration of the main workflow should NOT propagate to the subworkflow.
     * @return This builder.
     */
    public SubWorkflowActionBuilder withoutPropagatingConfiguration() {
        this.propagateConfiguration.set(false);
        return this;
    }

    /**
     * Registers a configuration property (a key-value pair) with this builder. If the provided key has already been
     * set on this builder, an exception is thrown. Setting a key to null means deleting it.
     * @param key The name of the property to set.
     * @param value The value of the property to set.
     * @return this
     * @throws IllegalStateException if the provided key has already been set on this builder.
     */
    public SubWorkflowActionBuilder withConfigProperty(final String key, final String value) {
        ModifyOnce<String> mappedValue = this.configuration.get(key);

        if (mappedValue == null) {
            mappedValue = new ModifyOnce<>(value);
            this.configuration.put(key, mappedValue);
        }

        mappedValue.set(value);

        return this;
    }

    /**
     * Creates a new {@link SubWorkflowAction} object with the properties stores in this builder.
     * The new {@link MapReduceAction} object is independent of this builder and the builder can be used to build
     * new instances.
     * @return A new {@link MapReduceAction} object with the properties stored in this builder.
     */
    @Override
    public SubWorkflowAction build() {
        final Node.ConstructionData constructionData = getConstructionData();

        final SubWorkflowAction instance = new SubWorkflowAction(
                constructionData,
                appPath.get(),
                propagateConfiguration.get(),
                ActionAttributesBuilder.convertToConfigurationMap(configuration));

        addAsChildToAllParents(instance);

        return instance;
    }

    @Override
    protected SubWorkflowActionBuilder getRuntimeSelfReference() {
        return this;
    }
}
