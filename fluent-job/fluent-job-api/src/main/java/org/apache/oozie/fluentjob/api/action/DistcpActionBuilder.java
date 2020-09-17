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
 * A builder class for {@link DistcpAction}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link DistcpActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DistcpActionBuilder extends NodeBuilderBaseImpl<DistcpActionBuilder> implements Builder<DistcpAction> {
    private final ActionAttributesBuilder attributesBuilder;

    public static DistcpActionBuilder create() {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.create();

        return new DistcpActionBuilder(
                null,
                builder);
    }

    public static DistcpActionBuilder createFromExistingAction(final Node action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromAction(action);

        return new DistcpActionBuilder(action,
                builder);
    }

    DistcpActionBuilder(final Node action,
                        final ActionAttributesBuilder attributesBuilder) {
        super(action);

        this.attributesBuilder = attributesBuilder;
    }

    public DistcpActionBuilder withResourceManager(final String resourceManager) {
        attributesBuilder.withResourceManager(resourceManager);
        return this;
    }

    public DistcpActionBuilder withNameNode(final String nameNode) {
        attributesBuilder.withNameNode(nameNode);
        return this;
    }

    public DistcpActionBuilder withPrepare(final Prepare prepare) {
        attributesBuilder.withPrepare(prepare);
        return this;
    }

    public DistcpActionBuilder withConfigProperty(final String key, final String value) {
        attributesBuilder.withConfigProperty(key, value);
        return this;
    }

    public DistcpActionBuilder withJavaOpts(final String javaOpts) {
        attributesBuilder.withJavaOpts(javaOpts);
        return this;
    }

    public DistcpActionBuilder withArg(final String arg) {
        attributesBuilder.withArg(arg);
        return this;
    }

    public DistcpActionBuilder withoutArg(final String arg) {
        attributesBuilder.withoutArg(arg);
        return this;
    }

    public DistcpActionBuilder clearArgs() {
        attributesBuilder.clearArgs();
        return this;
    }

    @Override
    public DistcpAction build() {
        final Node.ConstructionData constructionData = getConstructionData();

        final DistcpAction instance = new DistcpAction(
                constructionData,
                attributesBuilder.build());

        addAsChildToAllParents(instance);

        return instance;
    }

    @Override
    protected DistcpActionBuilder getRuntimeSelfReference() {
        return this;
    }
}
