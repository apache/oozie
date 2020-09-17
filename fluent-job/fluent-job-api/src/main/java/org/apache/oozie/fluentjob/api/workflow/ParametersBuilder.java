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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.oozie.fluentjob.api.action.Builder;

/**
 * A builder class for {@link Parameters}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link ParametersBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ParametersBuilder implements Builder<Parameters> {
    private final ImmutableList.Builder<Parameter> parameters;

    public static ParametersBuilder create() {
        return new ParametersBuilder(new ImmutableList.Builder<Parameter>());
    }

    public static ParametersBuilder createFromExisting(final Parameters parameters) {
        return new ParametersBuilder(new ImmutableList.Builder<Parameter>().addAll(parameters.getParameters()));
    }

    ParametersBuilder(final ImmutableList.Builder<Parameter> parameters) {
        this.parameters = parameters;
    }

    public ParametersBuilder withParameter(final String name, final String value) {
        return withParameter(name, value, null);
    }

    public ParametersBuilder withParameter(final String name, final String value, final String description) {
        parameters.add(new Parameter(name, value, description));
        return this;
    }

    @Override
    public Parameters build() {
        return new Parameters(parameters.build());
    }
}
