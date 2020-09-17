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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.oozie.fluentjob.api.ModifyOnce;

/**
 * A builder class for {@link GitAction}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link GitActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GitActionBuilder extends NodeBuilderBaseImpl<GitActionBuilder> implements Builder<GitAction> {
    private final ActionAttributesBuilder attributesBuilder;
    private final ModifyOnce<String> gitUri;
    private final ModifyOnce<String> branch;
    private final ModifyOnce<String> keyPath;
    private final ModifyOnce<String> destinationUri;

    public static GitActionBuilder create() {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.create();
        final ModifyOnce<String> gitUri = new ModifyOnce<>();
        final ModifyOnce<String> branch = new ModifyOnce<>();
        final ModifyOnce<String> keyPath = new ModifyOnce<>();
        final ModifyOnce<String> destinationUri = new ModifyOnce<>();

        return new GitActionBuilder(
                null,
                builder,
                gitUri,
                branch,
                keyPath,
                destinationUri);
    }

    public static GitActionBuilder createFromExistingAction(final GitAction action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromExisting(action.getAttributes());
        final ModifyOnce<String> gitUri = new ModifyOnce<>(action.getGitUri());
        final ModifyOnce<String> branch = new ModifyOnce<>(action.getBranch());
        final ModifyOnce<String> keyPath = new ModifyOnce<>(action.getKeyPath());
        final ModifyOnce<String> destinationUri = new ModifyOnce<>(action.getDestinationUri());

        return new GitActionBuilder(
                action,
                builder,
                gitUri,
                branch,
                keyPath,
                destinationUri);
    }

    public static GitActionBuilder createFromExistingAction(final Node action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromAction(action);
        final ModifyOnce<String> gitUri = new ModifyOnce<>();
        final ModifyOnce<String> branch = new ModifyOnce<>();
        final ModifyOnce<String> keyPath = new ModifyOnce<>();
        final ModifyOnce<String> destinationUri = new ModifyOnce<>();

        return new GitActionBuilder(
                action,
                builder,
                gitUri,
                branch,
                keyPath,
                destinationUri);
    }

    private GitActionBuilder(final Node action,
                     final ActionAttributesBuilder attributesBuilder,
                     final ModifyOnce<String> gitUri,
                     final ModifyOnce<String> branch,
                     final ModifyOnce<String> keyPath,
                     final ModifyOnce<String> destinationUri) {
        super(action);

        this.attributesBuilder = attributesBuilder;
        this.gitUri = gitUri;
        this.branch = branch;
        this.keyPath = keyPath;
        this.destinationUri = destinationUri;
    }

    public GitActionBuilder withResourceManager(final String resourceManager) {
        this.attributesBuilder.withResourceManager(resourceManager);
        return this;
    }

    public GitActionBuilder withNameNode(final String nameNode) {
        this.attributesBuilder.withNameNode(nameNode);
        return this;
    }

    public GitActionBuilder withPrepare(final Prepare prepare) {
        this.attributesBuilder.withPrepare(prepare);
        return this;
    }

    public GitActionBuilder withConfigProperty(final String key, final String value) {
        this.attributesBuilder.withConfigProperty(key, value);
        return this;
    }

    public GitActionBuilder withGitUri(final String gitUri) {
        this.gitUri.set(gitUri);
        return this;
    }

    public GitActionBuilder withBranch(final String branch) {
        this.branch.set(branch);
        return this;
    }

    public GitActionBuilder withKeyPath(final String keyPath) {
        this.keyPath.set(keyPath);
        return this;
    }

    public GitActionBuilder withDestinationUri(final String destinationUri) {
        this.destinationUri.set(destinationUri);
        return this;
    }

    @Override
    public GitAction build() {
        final Node.ConstructionData constructionData = getConstructionData();

        final GitAction instance = new GitAction(
                constructionData,
                attributesBuilder.build(),
                gitUri.get(),
                branch.get(),
                keyPath.get(),
                destinationUri.get());

        addAsChildToAllParents(instance);

        return instance;
    }

    @Override
    protected GitActionBuilder getRuntimeSelfReference() {
        return this;
    }
}
