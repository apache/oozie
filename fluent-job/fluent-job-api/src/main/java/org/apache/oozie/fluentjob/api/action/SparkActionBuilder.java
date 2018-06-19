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
 * A builder class for {@link SparkAction}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link SparkActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SparkActionBuilder extends NodeBuilderBaseImpl<SparkActionBuilder> implements Builder<SparkAction> {
    private final ActionAttributesBuilder attributesBuilder;
    private final ModifyOnce<String> master;
    private final ModifyOnce<String> mode;
    private final ModifyOnce<String> actionName;
    private final ModifyOnce<String> actionClass;
    private final ModifyOnce<String> jar;
    private final ModifyOnce<String> sparkOpts;

    public static SparkActionBuilder create() {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.create();
        final ModifyOnce<String> master = new ModifyOnce<>();
        final ModifyOnce<String> mode = new ModifyOnce<>();
        final ModifyOnce<String> actionName = new ModifyOnce<>();
        final ModifyOnce<String> actionClass = new ModifyOnce<>();
        final ModifyOnce<String> jar = new ModifyOnce<>();
        final ModifyOnce<String> sparkOpts = new ModifyOnce<>();

        return new SparkActionBuilder(
                null,
                builder,
                master,
                mode,
                actionName,
                actionClass,
                jar,
                sparkOpts);
    }

    public static SparkActionBuilder createFromExistingAction(final SparkAction action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromExisting(action.getAttributes());
        final ModifyOnce<String> master = new ModifyOnce<>(action.getMaster());
        final ModifyOnce<String> mode = new ModifyOnce<>(action.getMode());
        final ModifyOnce<String> actionName = new ModifyOnce<>(action.getActionName());
        final ModifyOnce<String> actionClass = new ModifyOnce<>(action.getActionClass());
        final ModifyOnce<String> jar = new ModifyOnce<>(action.getJar());
        final ModifyOnce<String> sparkOpts = new ModifyOnce<>(action.getSparkOpts());

        return new SparkActionBuilder(action,
                builder,
                master,
                mode,
                actionName,
                actionClass,
                jar,
                sparkOpts);
    }

    public static SparkActionBuilder createFromExistingAction(final Node action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromAction(action);
        final ModifyOnce<String> master = new ModifyOnce<>();
        final ModifyOnce<String> mode = new ModifyOnce<>();
        final ModifyOnce<String> actionName = new ModifyOnce<>();
        final ModifyOnce<String> actionClass = new ModifyOnce<>();
        final ModifyOnce<String> jar = new ModifyOnce<>();
        final ModifyOnce<String> sparkOpts = new ModifyOnce<>();

        return new SparkActionBuilder(action,
                builder,
                master,
                mode,
                actionName,
                actionClass,
                jar,
                sparkOpts);
    }

    SparkActionBuilder(final Node action,
                       final ActionAttributesBuilder attributesBuilder,
                       final ModifyOnce<String> master,
                       final ModifyOnce<String> mode,
                       final ModifyOnce<String> actionName,
                       final ModifyOnce<String> actionClass,
                       final ModifyOnce<String> jar,
                       final ModifyOnce<String> sparkOpts) {
        super(action);

        this.attributesBuilder = attributesBuilder;
        this.master = master;
        this.mode = mode;
        this.actionName = actionName;
        this.actionClass = actionClass;
        this.jar = jar;
        this.sparkOpts = sparkOpts;
    }

    public SparkActionBuilder withResourceManager(final String resourceManager) {
        this.attributesBuilder.withResourceManager(resourceManager);
        return this;
    }

    public SparkActionBuilder withNameNode(final String nameNode) {
        this.attributesBuilder.withNameNode(nameNode);
        return this;
    }

    public SparkActionBuilder withPrepare(final Prepare prepare) {
        this.attributesBuilder.withPrepare(prepare);
        return this;
    }

    public SparkActionBuilder withLauncher(final Launcher launcher) {
        this.attributesBuilder.withLauncher(launcher);
        return this;
    }

    public SparkActionBuilder withJobXml(final String jobXml) {
        this.attributesBuilder.withJobXml(jobXml);
        return this;
    }

    public SparkActionBuilder withoutJobXml(final String jobXml) {
        this.attributesBuilder.withoutJobXml(jobXml);
        return this;
    }

    public SparkActionBuilder clearJobXmls() {
        this.attributesBuilder.clearJobXmls();
        return this;
    }

    public SparkActionBuilder withConfigProperty(final String key, final String value) {
        this.attributesBuilder.withConfigProperty(key, value);
        return this;
    }

    public SparkActionBuilder withMaster(final String master) {
        this.master.set(master);
        return this;
    }

    public SparkActionBuilder withMode(final String mode) {
        this.mode.set(mode);
        return this;
    }

    public SparkActionBuilder withActionName(final String actionName) {
        this.actionName.set(actionName);
        return this;
    }

    public SparkActionBuilder withActionClass(final String actionClass) {
        this.actionClass.set(actionClass);
        return this;
    }

    public SparkActionBuilder withJar(final String jar) {
        this.jar.set(jar);
        return this;
    }

    public SparkActionBuilder withSparkOpts(final String sparkOpts) {
        this.sparkOpts.set(sparkOpts);
        return this;
    }

    public SparkActionBuilder withArg(final String arg) {
        this.attributesBuilder.withArg(arg);
        return this;
    }

    public SparkActionBuilder withoutArg(final String arg) {
        this.attributesBuilder.withoutArg(arg);
        return this;
    }

    public SparkActionBuilder clearArgs() {
        this.attributesBuilder.clearArgs();
        return this;
    }

    public SparkActionBuilder withFile(final String file) {
        this.attributesBuilder.withFile(file);
        return this;
    }

    public SparkActionBuilder withoutFile(final String file) {
        this.attributesBuilder.withoutFile(file);
        return this;
    }

    public SparkActionBuilder clearFiles() {
        this.attributesBuilder.clearFiles();
        return this;
    }

    public SparkActionBuilder withArchive(final String archive) {
        this.attributesBuilder.withArchive(archive);
        return this;
    }

    public SparkActionBuilder withoutArchive(final String archive) {
        this.attributesBuilder.withoutArchive(archive);
        return this;
    }

    public SparkActionBuilder clearArchive() {
        this.attributesBuilder.clearArchives();
        return this;
    }

    @Override
    public SparkAction build() {
        final Node.ConstructionData constructionData = getConstructionData();

        final SparkAction instance = new SparkAction(
                constructionData,
                attributesBuilder.build(),
                master.get(),
                mode.get(),
                actionName.get(),
                actionClass.get(),
                jar.get(),
                sparkOpts.get());

        addAsChildToAllParents(instance);

        return instance;
    }

    @Override
    protected SparkActionBuilder getRuntimeSelfReference() {
        return this;
    }
}
