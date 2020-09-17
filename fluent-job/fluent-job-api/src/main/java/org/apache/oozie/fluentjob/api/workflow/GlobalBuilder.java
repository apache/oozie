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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.oozie.fluentjob.api.action.ActionAttributesBuilder;
import org.apache.oozie.fluentjob.api.action.Builder;
import org.apache.oozie.fluentjob.api.action.HasAttributes;
import org.apache.oozie.fluentjob.api.action.Launcher;

/**
 * A builder class for {@link Global}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link GlobalBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GlobalBuilder implements Builder<Global> {
    private final ActionAttributesBuilder attributesBuilder;

    public static GlobalBuilder create() {
        return new GlobalBuilder(ActionAttributesBuilder.create());
    }

    public static GlobalBuilder createFromExisting(final HasAttributes hasAttributes) {
        return new GlobalBuilder(ActionAttributesBuilder.createFromExisting(hasAttributes.getAttributes()));
    }

    private GlobalBuilder(final ActionAttributesBuilder attributesBuilder) {
        this.attributesBuilder = attributesBuilder;
    }

    public GlobalBuilder withResourceManager(final String resourceManager) {
        attributesBuilder.withResourceManager(resourceManager);
        return this;
    }

    public GlobalBuilder withNameNode(final String nameNode) {
        attributesBuilder.withNameNode(nameNode);
        return this;
    }

    public GlobalBuilder withLauncher(final Launcher launcher) {
        attributesBuilder.withLauncher(launcher);
        return this;
    }

    public GlobalBuilder withJobXml(final String jobXml) {
        attributesBuilder.withJobXml(jobXml);
        return this;
    }

    public GlobalBuilder withoutJobXml(final String jobXml) {
        attributesBuilder.withoutJobXml(jobXml);
        return this;
    }

    public GlobalBuilder clearJobXmls() {
        attributesBuilder.clearJobXmls();
        return this;
    }

    public GlobalBuilder withConfigProperty(final String key, final String value) {
        attributesBuilder.withConfigProperty(key, value);
        return this;
    }

    @Override
    public Global build() {
        return new Global(attributesBuilder.build());
    }
}
