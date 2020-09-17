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

/**
 * A builder class for {@link SqoopAction}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link SqoopActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SqoopActionBuilder extends NodeBuilderBaseImpl<SqoopActionBuilder> implements Builder<SqoopAction> {
    private final ActionAttributesBuilder attributesBuilder;
    private final ModifyOnce<String> command;

    public static SqoopActionBuilder create() {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.create();
        final ModifyOnce<String> command = new ModifyOnce<>();

        return new SqoopActionBuilder(
                null,
                builder,
                command);
    }

    public static SqoopActionBuilder createFromExistingAction(final SqoopAction action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromExisting(action.getAttributes());
        final ModifyOnce<String> command = new ModifyOnce<>(action.getCommand());

        return new SqoopActionBuilder(action,
                builder,
                command);
    }

    public static SqoopActionBuilder createFromExistingAction(final Node action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromAction(action);
        final ModifyOnce<String> command = new ModifyOnce<>();

        return new SqoopActionBuilder(action,
                builder,
                command);
    }

    private SqoopActionBuilder(final Node action,
                               final ActionAttributesBuilder attributesBuilder,
                               final ModifyOnce<String> command) {
        super(action);

        this.attributesBuilder = attributesBuilder;
        this.command = command;
    }

    public SqoopActionBuilder withResourceManager(final String resourceManager) {
        this.attributesBuilder.withResourceManager(resourceManager);
        return this;
    }

    public SqoopActionBuilder withNameNode(final String nameNode) {
        this.attributesBuilder.withNameNode(nameNode);
        return this;
    }

    public SqoopActionBuilder withPrepare(final Prepare prepare) {
        this.attributesBuilder.withPrepare(prepare);
        return this;
    }

    public SqoopActionBuilder withLauncher(final Launcher launcher) {
        this.attributesBuilder.withLauncher(launcher);
        return this;
    }

    public SqoopActionBuilder withJobXml(final String jobXml) {
        this.attributesBuilder.withJobXml(jobXml);
        return this;
    }

    public SqoopActionBuilder withoutJobXml(final String jobXml) {
        this.attributesBuilder.withoutJobXml(jobXml);
        return this;
    }

    public SqoopActionBuilder clearJobXmls() {
        this.attributesBuilder.clearJobXmls();
        return this;
    }

    public SqoopActionBuilder withConfigProperty(final String key, final String value) {
        this.attributesBuilder.withConfigProperty(key, value);
        return this;
    }

    public SqoopActionBuilder withCommand(final String command) {
        this.command.set(command);
        return this;
    }

    public SqoopActionBuilder withArgument(final String argument) {
        this.attributesBuilder.withArg(argument);
        return this;
    }

    public SqoopActionBuilder withoutArgument(final String argument) {
        this.attributesBuilder.withoutArg(argument);
        return this;
    }

    public SqoopActionBuilder clearArguments() {
        this.attributesBuilder.clearArgs();
        return this;
    }

    public SqoopActionBuilder withFile(final String file) {
        this.attributesBuilder.withFile(file);
        return this;
    }

    public SqoopActionBuilder withoutFile(final String file) {
        this.attributesBuilder.withoutFile(file);
        return this;
    }

    public SqoopActionBuilder clearFiles() {
        this.attributesBuilder.clearFiles();
        return this;
    }

    public SqoopActionBuilder withArchive(final String archive) {
        this.attributesBuilder.withArchive(archive);
        return this;
    }

    public SqoopActionBuilder withoutArchive(final String archive) {
        this.attributesBuilder.withoutArchive(archive);
        return this;
    }

    public SqoopActionBuilder clearArchives() {
        this.attributesBuilder.clearArchives();
        return this;
    }

    @Override
    public SqoopAction build() {
        final Node.ConstructionData constructionData = getConstructionData();

        final SqoopAction instance = new SqoopAction(
                constructionData,
                attributesBuilder.build(),
                command.get());

        addAsChildToAllParents(instance);

        return instance;
    }

    @Override
    protected SqoopActionBuilder getRuntimeSelfReference() {
        return this;
    }
}
