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
import org.apache.oozie.fluentjob.api.ModifyOnce;
import org.apache.oozie.fluentjob.api.action.Builder;

import java.util.ArrayList;
import java.util.List;

/**
 * A builder class for {@link Credential}.
 * <p>
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 * <p>
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link CredentialBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CredentialBuilder implements Builder<Credential> {
    private final ModifyOnce<String> name;
    private final ModifyOnce<String> type;
    private final List<ConfigurationEntry> configurationEntries;

    public static CredentialBuilder create() {
        final ModifyOnce<String> name = new ModifyOnce<>();
        final ModifyOnce<String> type = new ModifyOnce<>();
        final List<ConfigurationEntry> configurationEntries = new ArrayList<>();

        return new CredentialBuilder(name, type, configurationEntries);
    }

    public static CredentialBuilder createFromExisting(final Credential credential) {
        return new CredentialBuilder(new ModifyOnce<>(credential.getName()),
                new ModifyOnce<>(credential.getType()),
                new ArrayList<>(credential.getConfigurationEntries()));
    }

    private CredentialBuilder(final ModifyOnce<String> name,
            final ModifyOnce<String> type,
            final List<ConfigurationEntry> configurationEntries) {
        this.name = name;
        this.type = type;
        this.configurationEntries = configurationEntries;
    }

    public CredentialBuilder withName(final String name) {
        this.name.set(name);
        return this;
    }

    public CredentialBuilder withType(final String type) {
        this.type.set(type);
        return this;
    }

    public CredentialBuilder withConfigurationEntry(final String name,
                                                    final String description) {
        this.configurationEntries.add(new ConfigurationEntry(name, description));
        return this;
    }

    @Override
    public Credential build() {
        return new Credential(name.get(), type.get(), ImmutableList.copyOf(configurationEntries));
    }
}
