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
 * A builder class for {@link JavaAction}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link JavaActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JavaActionBuilder extends NodeBuilderBaseImpl<JavaActionBuilder> implements Builder<JavaAction> {
    private final ActionAttributesBuilder attributesBuilder;
    private final ModifyOnce<String> mainClass;
    private final ModifyOnce<String> javaOptsString;
    private final List<String> javaOpts;

    public static JavaActionBuilder create() {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.create();
        final ModifyOnce<String> mainClass = new ModifyOnce<>();
        final ModifyOnce<String> javaOptsString = new ModifyOnce<>();
        final List<String> javaOpts = new ArrayList<>();

        return new JavaActionBuilder(
                null,
                builder,
                mainClass,
                javaOptsString,
                javaOpts);
    }

    public static JavaActionBuilder createFromExistingAction(final JavaAction action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromExisting(action.getAttributes());
        final ModifyOnce<String> mainClass = new ModifyOnce<>(action.getMainClass());
        final ModifyOnce<String> javaOptsString = new ModifyOnce<>(action.getJavaOptsString());
        final List<String> javaOpts = new ArrayList<>(action.getJavaOpts());

        return new JavaActionBuilder(action,
                builder,
                mainClass,
                javaOptsString,
                javaOpts);
    }

    public static JavaActionBuilder createFromExistingAction(final Node action) {
        final ActionAttributesBuilder builder = ActionAttributesBuilder.createFromAction(action);
        final ModifyOnce<String> mainClass = new ModifyOnce<>();
        final ModifyOnce<String> javaOptsString = new ModifyOnce<>();
        final List<String> javaOpts = new ArrayList<>();

        return new JavaActionBuilder(action,
                builder,
                mainClass,
                javaOptsString,
                javaOpts);
    }

    private JavaActionBuilder(final Node action,
                              final ActionAttributesBuilder attributesBuilder,
                              final ModifyOnce<String> mainClass,
                              final ModifyOnce<String> javaOptsString,
                              final List<String> javaOpts) {
        super(action);

        this.attributesBuilder = attributesBuilder;
        this.mainClass = mainClass;
        this.javaOptsString = javaOptsString;
        this.javaOpts = javaOpts;
    }

    public JavaActionBuilder withResourceManager(final String resourceManager) {
        this.attributesBuilder.withResourceManager(resourceManager);
        return this;
    }

    public JavaActionBuilder withNameNode(final String nameNode) {
        this.attributesBuilder.withNameNode(nameNode);
        return this;
    }

    public JavaActionBuilder withPrepare(final Prepare prepare) {
        this.attributesBuilder.withPrepare(prepare);
        return this;
    }

    public JavaActionBuilder withLauncher(final Launcher launcher) {
        this.attributesBuilder.withLauncher(launcher);
        return this;
    }

    public JavaActionBuilder withJobXml(final String jobXml) {
        this.attributesBuilder.withJobXml(jobXml);
        return this;
    }

    public JavaActionBuilder withoutJobXml(final String jobXml) {
        this.attributesBuilder.withoutJobXml(jobXml);
        return this;
    }

    public JavaActionBuilder clearJobXmls() {
        this.attributesBuilder.clearJobXmls();
        return this;
    }

    public JavaActionBuilder withConfigProperty(final String key, final String value) {
        this.attributesBuilder.withConfigProperty(key, value);
        return this;
    }

    public JavaActionBuilder withMainClass(final String mainClass) {
        this.mainClass.set(mainClass);
        return this;
    }

    public JavaActionBuilder withJavaOptsString(final String javaOptsString) {
        this.javaOptsString.set(javaOptsString);
        return this;
    }

    public JavaActionBuilder withJavaOpt(final String javaOpt) {
        this.javaOpts.add(javaOpt);
        return this;
    }

    public JavaActionBuilder withoutJavaOpt(final String javaOpt) {
        this.javaOpts.remove(javaOpt);
        return this;
    }

    public JavaActionBuilder clearJavaOpts() {
        this.javaOpts.clear();
        return this;
    }

    public JavaActionBuilder withArg(final String arg) {
        this.attributesBuilder.withArg(arg);
        return this;
    }

    public JavaActionBuilder withoutArg(final String arg) {
        this.attributesBuilder.withoutArg(arg);
        return this;
    }

    public JavaActionBuilder clearArgs() {
        this.attributesBuilder.clearArgs();
        return this;
    }

    public JavaActionBuilder withFile(final String file) {
        this.attributesBuilder.withFile(file);
        return this;
    }

    public JavaActionBuilder withoutFile(final String file) {
        this.attributesBuilder.withoutFile(file);
        return this;
    }

    public JavaActionBuilder clearFiles() {
        this.attributesBuilder.clearFiles();
        return this;
    }

    public JavaActionBuilder withArchive(final String archive) {
        this.attributesBuilder.withArchive(archive);
        return this;
    }

    public JavaActionBuilder withoutArchive(final String archive) {
        this.attributesBuilder.withoutArchive(archive);
        return this;
    }

    public JavaActionBuilder clearArchives() {
        this.attributesBuilder.clearArchives();
        return this;
    }

    public JavaActionBuilder withCaptureOutput(final Boolean captureOutput) {
        this.attributesBuilder.withCaptureOutput(captureOutput);
        return this;
    }

    @Override
    public JavaAction build() {
        final Node.ConstructionData constructionData = getConstructionData();

        final JavaAction instance = new JavaAction(
                constructionData,
                attributesBuilder.build(),
                mainClass.get(),
                javaOptsString.get(),
                ImmutableList.copyOf(javaOpts));

        addAsChildToAllParents(instance);

        return instance;
    }

    @Override
    protected JavaActionBuilder getRuntimeSelfReference() {
        return this;
    }
}
