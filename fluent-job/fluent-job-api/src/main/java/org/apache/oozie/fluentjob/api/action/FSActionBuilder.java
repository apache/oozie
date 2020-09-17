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
 * A builder class for {@link FSAction}.
 * <p>
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 * <p>
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link FSActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FSActionBuilder extends NodeBuilderBaseImpl<FSActionBuilder> implements Builder<FSAction> {
    private final ActionAttributesBuilder attributesBuilder;

    /**
     * Creates and returns an empty builder.
     * @return An empty builder.
     */
    public static FSActionBuilder create() {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.create();

        return new FSActionBuilder(
                null,
                builder);
    }

    /**
     * Create and return a new {@link FSActionBuilder} that is based on an already built
     * {@link FSAction} object. The properties of the builder will initially be the same as those of the
     * provided {@link FSAction} object, but it is possible to modify them once.
     * @param action The {@link FSAction} object on which this {@link FSActionBuilder} will be based.
     * @return A new {@link FSActionBuilder} that is based on a previously built {@link FSAction} object.
     */
    public static FSActionBuilder createFromExistingAction(final Node action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromAction(action);

        return new FSActionBuilder(
                action,
                builder);
    }

    FSActionBuilder(final Node action,
                    final ActionAttributesBuilder attributesBuilder) {
        super(action);

        this.attributesBuilder = attributesBuilder;
    }

    /**
     * Registers a name node.
     * @param nameNode The string representing the name node.
     * @return this
     * @throws IllegalStateException if a name node has already been set on this builder.
     */
    public FSActionBuilder withNameNode(final String nameNode) {
        attributesBuilder.withNameNode(nameNode);
        return this;
    }

    /**
     * Registers a job XML with this builder.
     * @param jobXml The job XML to register.
     * @return this
     */
    public FSActionBuilder withJobXml(final String jobXml) {
        attributesBuilder.withJobXml(jobXml);
        return this;
    }

    /**
     * Removes a job XML if it is registered with this builder, otherwise does nothing.
     * @param jobXml The job XML to remove.
     * @return this
     */
    public FSActionBuilder withoutJobXml(final String jobXml) {
        attributesBuilder.withoutJobXml(jobXml);
        return this;
    }

    /**
     * Removes all job XMLs that are registered with this builder.
     * @return this
     */
    public FSActionBuilder clearJobXmls() {
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
    public FSActionBuilder withConfigProperty(final String key, final String value) {
        attributesBuilder.withConfigProperty(key, value);
        return this;
    }

    /**
     * Registers a {@link Delete} object with this builder.
     * @param delete The {@link Delete} object to register.
     * @return this
     */
    public FSActionBuilder withDelete(final Delete delete) {
        attributesBuilder.withDelete(delete);
        return this;
    }

    /**
     * Removes a {@link Delete} object if it is registered with this builder, otherwise does nothing.
     * @param delete The {@link Delete} object to remove.
     * @return this
     */
    public FSActionBuilder withoutDelete(final Delete delete) {
        attributesBuilder.withoutDelete(delete);
        return this;
    }

    /**
     * Removes all {@link Delete} objects that are registered with this builder.
     * @return this
     */
    public FSActionBuilder clearDeletes() {
        attributesBuilder.clearDeletes();
        return this;
    }

    /**
     * Registers a {@link Mkdir} object with this builder.
     * @param mkdir The {@link Mkdir} object to register.
     * @return this
     */
    public FSActionBuilder withMkdir(final Mkdir mkdir) {
        attributesBuilder.withMkdir(mkdir);
        return this;
    }

    /**
     * Removes a {@link Mkdir} object if it is registered with this builder, otherwise does nothing.
     * @param mkdir The {@link Mkdir} object to remove.
     * @return this
     */
    public FSActionBuilder withoutMkdir(final Mkdir mkdir) {
        attributesBuilder.withoutMkdir(mkdir);
        return this;
    }

    /**
     * Removes all {@link Mkdir} objects that are registered with this builder.
     * @return this
     */
    public FSActionBuilder clearMkdirs() {
        attributesBuilder.clearMkdirs();
        return this;
    }

    /**
     * Registers a {@link Move} object with this builder.
     * @param move The {@link Move} object to register.
     * @return this
     */
    public FSActionBuilder withMove(final Move move) {
        attributesBuilder.withMove(move);
        return this;
    }

    /**
     * Removes a {@link Move} object if it is registered with this builder, otherwise does nothing.
     * @param move The {@link Move} object to remove.
     * @return this
     */
    public FSActionBuilder withoutMove(final Move move) {
        attributesBuilder.withoutMove(move);
        return this;
    }

    /**
     * Removes all {@link Move} objects that are registered with this builder.
     * @return this
     */
    public FSActionBuilder clearMoves() {
        attributesBuilder.clearMoves();
        return this;
    }

    /**
     * Registers a {@link Chmod} object with this builder.
     * @param chmod The {@link Chmod} object to register.
     * @return this
     */
    public FSActionBuilder withChmod(final Chmod chmod) {
        attributesBuilder.withChmod(chmod);
        return this;
    }

    /**
     * Removes a {@link Chmod} object if it is registered with this builder, otherwise does nothing.
     * @param chmod The {@link Chmod} object to remove.
     * @return this
     */
    public FSActionBuilder withoutChmod(final Chmod chmod) {
        attributesBuilder.withoutChmod(chmod);
        return this;
    }

    /**
     * Removes all {@link Chmod} objects that are registered with this builder.
     * @return this
     */
    public FSActionBuilder clearChmods() {
        attributesBuilder.clearChmods();
        return this;
    }

    /**
     * Registers a {@link Touchz} object with this builder.
     * @param touchz The {@link Touchz} object to register.
     * @return this
     */
    public FSActionBuilder withTouchz(final Touchz touchz) {
        attributesBuilder.withTouchz(touchz);
        return this;
    }

    /**
     * Removes a {@link Touchz} object if it is registered with this builder, otherwise does nothing.
     * @param touchz The {@link Touchz} object to remove.
     * @return this
     */
    public FSActionBuilder withoutTouchz(final Touchz touchz) {
        attributesBuilder.withoutTouchz(touchz);
        return this;
    }

    /**
     * Removes all {@link Touchz} objects that are registered with this builder.
     * @return this
     */
    public FSActionBuilder clearTouchzs() {
        attributesBuilder.clearTouchzs();
        return this;
    }

    /**
     * Registers a {@link Chgrp} object with this builder.
     * @param chgrp The {@link Chgrp} object to register.
     * @return this
     */
    public FSActionBuilder withChgrp(final Chgrp chgrp) {
        attributesBuilder.withChgrp(chgrp);
        return this;
    }

    /**
     * Removes a {@link Chgrp} object if it is registered with this builder, otherwise does nothing.
     * @param chgrp The {@link Chgrp} object to remove.
     * @return this
     */
    public FSActionBuilder withoutChgrp(final Chgrp chgrp) {
        attributesBuilder.withoutChgrp(chgrp);
        return this;
    }

    /**
     * Removes all {@link Chgrp} objects that are registered with this builder.
     * @return this
     */
    public FSActionBuilder clearChgrps() {
        attributesBuilder.clearChgrps();
        return this;
    }

    /**
     * Creates a new {@link FSAction} object with the properties stores in this builder.
     * The new {@link FSAction} object is independent of this builder and the builder can be used to build
     * new instances.
     * @return A new {@link FSAction} object with the properties stored in this builder.
     */
    @Override
    public FSAction build() {
        final Node.ConstructionData constructionData = getConstructionData();

        final FSAction instance = new FSAction(
                constructionData,
                attributesBuilder.build());

        addAsChildToAllParents(instance);

        return instance;
    }

    @Override
    protected FSActionBuilder getRuntimeSelfReference() {
        return this;
    }
}
