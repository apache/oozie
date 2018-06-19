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

import java.util.List;

/**
 * A builder class for {@link Credentials}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link CredentialsBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CredentialsBuilder implements Builder<Credentials> {
    private final ImmutableList.Builder<Credential> credentials;

    public static CredentialsBuilder create() {
        return new CredentialsBuilder(new ImmutableList.Builder<Credential>());
    }

    public static CredentialsBuilder createFromExisting(final Credentials credentials) {
        return new CredentialsBuilder(new ImmutableList.Builder<Credential>().addAll(credentials.getCredentials()));
    }

    CredentialsBuilder(final ImmutableList.Builder<Credential> credentials) {
        this.credentials = credentials;
    }

    public CredentialsBuilder withCredential(final String name,
                                             final String value) {
        this.credentials.add(new Credential(name, value, ImmutableList.<ConfigurationEntry>of()));
        return this;
    }

    public CredentialsBuilder withCredential(final String name,
                                             final String type,
                                             final List<ConfigurationEntry> configurationEntries) {
        this.credentials.add(new Credential(name, type, ImmutableList.copyOf(configurationEntries)));
        return this;
    }

    public CredentialsBuilder withCredential(final Credential credential) {
        this.credentials.add(credential);
        return this;
    }

    @Override
    public Credentials build() {
        return new Credentials(credentials.build());
    }
}
