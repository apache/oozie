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

import java.util.ArrayList;
import java.util.List;

/**
 * A builder class for {@link Hive2Action}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link Hive2ActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Hive2ActionBuilder extends NodeBuilderBaseImpl<Hive2ActionBuilder> implements Builder<Hive2Action> {
    private final HiveActionBuilder delegate;
    private final ModifyOnce<String> jdbcUrl;
    private final ModifyOnce<String> password;

    public static Hive2ActionBuilder create() {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.create();
        final ModifyOnce<String> jdbcUrl = new ModifyOnce<>();
        final ModifyOnce<String> password = new ModifyOnce<>();
        final ModifyOnce<String> script = new ModifyOnce<>();
        final ModifyOnce<String> query = new ModifyOnce<>();
        final List<String> params = new ArrayList<>();

        return new Hive2ActionBuilder(
                null,
                builder,
                jdbcUrl,
                password,
                script,
                query,
                params);
    }

    public static Hive2ActionBuilder createFromExistingAction(final Hive2Action action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromExisting(action.getAttributes());
        final ModifyOnce<String> jdbcUrl = new ModifyOnce<>(action.getJdbcUrl());
        final ModifyOnce<String> password = new ModifyOnce<>(action.getPassword());
        final ModifyOnce<String> script = new ModifyOnce<>(action.getScript());
        final ModifyOnce<String> query = new ModifyOnce<>(action.getQuery());
        final List<String> params = new ArrayList<>(action.getParams());

        return new Hive2ActionBuilder(action,
                builder,
                jdbcUrl,
                password,
                script,
                query,
                params);
    }

    public static Hive2ActionBuilder createFromExistingAction(final Node action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromAction(action);
        final ModifyOnce<String> jdbcUrl = new ModifyOnce<>();
        final ModifyOnce<String> password = new ModifyOnce<>();
        final ModifyOnce<String> script = new ModifyOnce<>();
        final ModifyOnce<String> query = new ModifyOnce<>();
        final List<String> params = new ArrayList<>();

        return new Hive2ActionBuilder(action,
                builder,
                jdbcUrl,
                password,
                script,
                query,
                params);
    }

    Hive2ActionBuilder(final Node action,
                       final ActionAttributesBuilder attributesBuilder,
                       final ModifyOnce<String> jdbcUrl,
                       final ModifyOnce<String> password,
                       final ModifyOnce<String> script,
                       final ModifyOnce<String> query,
                       final List<String> params) {
        super(action);

        this.delegate = new HiveActionBuilder(action,
                attributesBuilder,
                script,
                query,
                params);

        this.jdbcUrl = jdbcUrl;
        this.password = password;
    }

    public Hive2ActionBuilder withResourceManager(final String resourceManager) {
        delegate.withResourceManager(resourceManager);
        return this;
    }

    public Hive2ActionBuilder withNameNode(final String nameNode) {
        delegate.withNameNode(nameNode);
        return this;
    }

    public Hive2ActionBuilder withPrepare(final Prepare prepare) {
        delegate.withPrepare(prepare);
        return this;
    }

    public Hive2ActionBuilder withLauncher(final Launcher launcher) {
        delegate.withLauncher(launcher);
        return this;
    }

    public Hive2ActionBuilder withJobXml(final String jobXml) {
        delegate.withJobXml(jobXml);
        return this;
    }

    public Hive2ActionBuilder withoutJobXml(final String jobXml) {
        delegate.withoutJobXml(jobXml);
        return this;
    }

    public Hive2ActionBuilder clearJobXmls() {
        delegate.clearJobXmls();
        return this;
    }

    public Hive2ActionBuilder withConfigProperty(final String key, final String value) {
        delegate.withConfigProperty(key, value);
        return this;
    }

    public Hive2ActionBuilder withScript(final String script) {
        delegate.withScript(script);
        return this;
    }

    public Hive2ActionBuilder withQuery(final String query) {
        delegate.withQuery(query);
        return this;
    }

    public Hive2ActionBuilder withParam(final String param) {
        delegate.withParam(param);
        return this;
    }

    public Hive2ActionBuilder withoutParam(final String param) {
        delegate.withoutParam(param);
        return this;
    }

    public Hive2ActionBuilder clearParams() {
        delegate.clearParams();
        return this;
    }

    public Hive2ActionBuilder withArg(final String arg) {
        delegate.withArg(arg);
        return this;
    }

    public Hive2ActionBuilder withoutArg(final String arg) {
        delegate.withoutArg(arg);
        return this;
    }

    public Hive2ActionBuilder clearArgs() {
        delegate.clearArgs();
        return this;
    }

    public Hive2ActionBuilder withFile(final String file) {
        delegate.withFile(file);
        return this;
    }

    public Hive2ActionBuilder withoutFile(final String file) {
        delegate.withoutFile(file);
        return this;
    }

    public Hive2ActionBuilder clearFiles() {
        delegate.clearFiles();
        return this;
    }

    public Hive2ActionBuilder withArchive(final String archive) {
        delegate.withArchive(archive);
        return this;
    }

    public Hive2ActionBuilder withoutArchive(final String archive) {
        delegate.withoutArchive(archive);
        return this;
    }

    public Hive2ActionBuilder clearArchives() {
        delegate.clearArchives();
        return this;
    }

    public Hive2ActionBuilder withJdbcUrl(final String jdbcUrl) {
        this.jdbcUrl.set(jdbcUrl);
        return this;
    }

    public Hive2ActionBuilder withPassword(final String password) {
        this.password.set(password);
        return this;
    }

    @Override
    public Hive2Action build() {
        final Node.ConstructionData constructionData = getConstructionData();

        final Hive2Action instance = new Hive2Action(
                constructionData,
                delegate.getAttributesBuilder().build(),
                jdbcUrl.get(),
                password.get(),
                delegate.getScript().get(),
                delegate.query.get(),
                ImmutableList.copyOf(delegate.getParams()));

        addAsChildToAllParents(instance);

        return instance;
    }

    @Override
    protected Hive2ActionBuilder getRuntimeSelfReference() {
        return this;
    }
}
