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

/**
 * A builder class for {@link MapReduceAction}.
 * <p>
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 * <p>
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link MapReduceActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MapReduceActionBuilder extends NodeBuilderBaseImpl<MapReduceActionBuilder> implements Builder<MapReduceAction> {
    private final ActionAttributesBuilder attributesBuilder;

    /**
     * Creates and returns an empty builder.
     * @return An empty builder.
     */
    public static MapReduceActionBuilder create() {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.create();

        return new MapReduceActionBuilder(
                null,
                builder);
    }

    /**
     * Create and return a new {@link MapReduceActionBuilder} that is based on an already built
     * {@link MapReduceAction} object. The properties of the builder will initially be the same as those of the
     * provided {@link MapReduceAction} object, but it is possible to modify them once.
     * @param action The {@link MapReduceAction} object on which this {@link MapReduceActionBuilder} will be based.
     * @return A new {@link MapReduceActionBuilder} that is based on a previously built {@link MapReduceAction} object.
     */
    public static MapReduceActionBuilder createFromExistingAction(final Node action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromAction(action);

        return new MapReduceActionBuilder(
                action,
                builder);
    }

    MapReduceActionBuilder(final Node action,
                           final ActionAttributesBuilder attributesBuilder) {
        super(action);

        this.attributesBuilder = attributesBuilder;
    }

    public MapReduceActionBuilder withResourceManager(final String resourceManager) {
        attributesBuilder.withResourceManager(resourceManager);
        return this;
    }

    /**
     * Registers a name node.
     * @param nameNode The string representing the name node.
     * @return this
     * @throws IllegalStateException if a name node has already been set on this builder.
     */
    public MapReduceActionBuilder withNameNode(final String nameNode) {
        attributesBuilder.withNameNode(nameNode);
        return this;
    }

    /**
     * Registers a {@link Prepare} object.
     * @param prepare The {@link Prepare} object to register.
     * @return this
     * @throws IllegalStateException if a {@link Prepare} object has already been set on this builder.
     */
    public MapReduceActionBuilder withPrepare(final Prepare prepare) {
        attributesBuilder.withPrepare(prepare);
        return this;
    }

    /**
     * Registers a {@link Streaming} object.
     * @param streaming The {@link Streaming} object to register.
     * @return this
     * @throws IllegalStateException if a {@link Streaming} object has already been set on this builder.
     */
    public MapReduceActionBuilder withStreaming(final Streaming streaming) {
        attributesBuilder.withStreaming(streaming);
        return this;
    }

    /**
     * Registers a {@link Pipes} object.
     * @param pipes The {@link Pipes} object to register.
     * @return this
     * @throws IllegalStateException if a {@link Pipes} object has already been set on this builder.
     */
    public MapReduceActionBuilder withPipes(final Pipes pipes) {
        attributesBuilder.withPipes(pipes);
        return this;
    }

    /**
     * Registers a job XML with this builder.
     * @param jobXml The job XML to register.
     * @return this
     */
    public MapReduceActionBuilder withJobXml(final String jobXml) {
        attributesBuilder.withJobXml(jobXml);
        return this;
    }

    /**
     * Removes a job XML if it is registered with this builder, otherwise does nothing.
     * @param jobXml The job XML to remove.
     * @return this
     */
    public MapReduceActionBuilder withoutJobXml(final String jobXml) {
        attributesBuilder.withoutJobXml(jobXml);
        return this;
    }

    /**
     * Removes all job XMLs that are registered with this builder.
     * @return this
     */
    public MapReduceActionBuilder clearJobXmls() {
        attributesBuilder.clearJobXmls();
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
    public MapReduceActionBuilder withConfigProperty(final String key, final String value) {
        attributesBuilder.withConfigProperty(key, value);
        return this;
    }

    /**
     * Registers a configuration class with this builder.
     * @param configClass The string representing the configuration class.
     * @return this
     * @throws IllegalStateException if a configuration class has already been set on this builder.
     */
    public MapReduceActionBuilder withConfigClass(final String configClass) {
        attributesBuilder.withConfigClass(configClass);
        return this;
    }

    /**
     * Registers a file with this builder.
     * @param file The file to register.
     * @return this
     */
    public MapReduceActionBuilder withFile(final String file) {
        attributesBuilder.withFile(file);
        return this;
    }

    /**
     * Removes a file if it is registered with this builder, otherwise does nothing.
     * @param file The file to remove.
     * @return this
     */
    public MapReduceActionBuilder withoutFile(final String file) {
        attributesBuilder.withoutFile(file);
        return this;
    }

    /**
     * Removes all files that are registered with this builder.
     * @return this
     */
    public MapReduceActionBuilder clearFiles() {
        attributesBuilder.clearFiles();
        return this;
    }

    /**
     * Registers an archive with this builder.
     * @param archive The archive to register.
     * @return this
     */
    public MapReduceActionBuilder withArchive(final String archive) {
        attributesBuilder.withArchive(archive);
        return this;
    }

    /**
     * Removes an archive if it is registered with this builder, otherwise does nothing.
     * @param archive The archive to remove.
     * @return this
     */
    public MapReduceActionBuilder withoutArchive(final String archive) {
        attributesBuilder.withoutArchive(archive);
        return this;
    }

    /**
     * Removes all archives that are registered with this builder.
     * @return this
     */
    public MapReduceActionBuilder clearArchives() {
        attributesBuilder.clearArchives();
        return this;
    }

    /**
     * Creates a new {@link MapReduceAction} object with the properties stores in this builder.
     * The new {@link MapReduceAction} object is independent of this builder and the builder can be used to build
     * new instances.
     * @return A new {@link MapReduceAction} object with the properties stored in this builder.
     */
    @Override
    public MapReduceAction build() {
        final Node.ConstructionData constructionData = getConstructionData();

        final MapReduceAction instance = new MapReduceAction(
                constructionData,
                attributesBuilder.build());

        addAsChildToAllParents(instance);

        return instance;
    }

    @Override
    protected MapReduceActionBuilder getRuntimeSelfReference() {
        return this;
    }
}