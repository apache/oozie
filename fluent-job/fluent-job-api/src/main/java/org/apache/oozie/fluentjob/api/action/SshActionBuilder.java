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
 * A builder class for {@link EmailAction}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link SshActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SshActionBuilder extends NodeBuilderBaseImpl<SshActionBuilder> implements Builder<SshAction> {
    private final ActionAttributesBuilder attributesBuilder;
    private final ModifyOnce<String> host;
    private final ModifyOnce<String> command;

    public static SshActionBuilder create() {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.create();
        final ModifyOnce<String> host = new ModifyOnce<>();
        final ModifyOnce<String> command = new ModifyOnce<>();

        return new SshActionBuilder(
                null,
                builder,
                host,
                command);
    }

    public static SshActionBuilder createFromExistingAction(final SshAction action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromExisting(action.getAttributes());
        final ModifyOnce<String> host = new ModifyOnce<>(action.getHost());
        final ModifyOnce<String> command = new ModifyOnce<>(action.getCommand());

        return new SshActionBuilder(action,
                builder,
                host,
                command);
    }

    public static SshActionBuilder createFromExistingAction(final Node action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromAction(action);
        final ModifyOnce<String> host = new ModifyOnce<>();
        final ModifyOnce<String> command = new ModifyOnce<>();

        return new SshActionBuilder(action,
                builder,
                host,
                command);
    }

    private SshActionBuilder(final Node action,
                             final ActionAttributesBuilder attributesBuilder,
                             final ModifyOnce<String> host,
                             final ModifyOnce<String> command) {
        super(action);

        this.attributesBuilder = attributesBuilder;
        this.host = host;
        this.command = command;
    }

    public SshActionBuilder withHost(final String host) {
        this.host.set(host);
        return this;
    }

    public SshActionBuilder withCommand(final String command) {
        this.command.set(command);
        return this;
    }

    public SshActionBuilder withArg(final String arg) {
        this.attributesBuilder.withArg(arg);
        return this;
    }

    public SshActionBuilder withoutArg(final String arg) {
        this.attributesBuilder.withoutArg(arg);
        return this;
    }

    public SshActionBuilder clearArgs() {
        this.attributesBuilder.clearArgs();
        return this;
    }

    public SshActionBuilder withCaptureOutput(final Boolean captureOutput) {
        this.attributesBuilder.withCaptureOutput(captureOutput);
        return this;
    }

    @Override
    public SshAction build() {
        final Node.ConstructionData constructionData = getConstructionData();

        final SshAction instance = new SshAction(
                constructionData,
                attributesBuilder.build(),
                host.get(),
                command.get());

        addAsChildToAllParents(instance);

        return instance;
    }

    @Override
    protected SshActionBuilder getRuntimeSelfReference() {
        return this;
    }
}
