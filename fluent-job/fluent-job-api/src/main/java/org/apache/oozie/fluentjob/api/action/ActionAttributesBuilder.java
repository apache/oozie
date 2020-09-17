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
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.oozie.fluentjob.api.ModifyOnce;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A builder class for {@link ActionAttributes}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link ActionAttributesBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ActionAttributesBuilder implements Builder<ActionAttributes> {
    private final ModifyOnce<String> resourceManager;
    private final ModifyOnce<String> nameNode;
    private final ModifyOnce<Prepare> prepare;
    private final ModifyOnce<Streaming> streaming;
    private final ModifyOnce<Pipes> pipes;
    private final List<String> jobXmls;
    private final Map<String, ModifyOnce<String>> configuration;
    private final ModifyOnce<String> configClass;
    private final List<String> files;
    private final List<String> archives;
    private final List<Delete> deletes;
    private final List<Mkdir> mkdirs;
    private final List<Move> moves;
    private final List<Chmod> chmods;
    private final List<Touchz> touchzs;
    private final List<Chgrp> chgrps;
    private final ModifyOnce<String> javaOpts;
    private final List<String> args;
    private final ModifyOnce<Launcher> launcher;
    private final ModifyOnce<Boolean> captureOutput;

    /**
     * Creates and returns an empty builder.
     * @return An empty builder.
     */
    public static ActionAttributesBuilder create() {
        final ModifyOnce<String> resourceManager = new ModifyOnce<>();
        final ModifyOnce<String> nameNode = new ModifyOnce<>();
        final ModifyOnce<Prepare> prepare = new ModifyOnce<>();
        final ModifyOnce<Streaming> streaming = new ModifyOnce<>();
        final ModifyOnce<Pipes> pipes = new ModifyOnce<>();
        final List<String> jobXmls = new ArrayList<>();
        final Map<String, ModifyOnce<String>> configuration = new LinkedHashMap<>();
        final ModifyOnce<String> configClass = new ModifyOnce<>();
        final List<String> files = new ArrayList<>();
        final List<String> archives = new ArrayList<>();
        final List<Delete> deletes = new ArrayList<>();
        final List<Mkdir> mkdirs = new ArrayList<>();
        final List<Move> moves = new ArrayList<>();
        final List<Chmod> chmods = new ArrayList<>();
        final List<Touchz> touchzs = new ArrayList<>();
        final List<Chgrp> chgrps = new ArrayList<>();
        final ModifyOnce<String> javaOpts = new ModifyOnce<>();
        final List<String> args = new ArrayList<>();
        final ModifyOnce<Launcher> launcher = new ModifyOnce<>();
        final ModifyOnce<Boolean> captureOutput = new ModifyOnce<>();

        return new ActionAttributesBuilder(
                resourceManager,
                prepare,
                streaming,
                pipes,
                jobXmls,
                configuration,
                configClass,
                files,
                archives,
                deletes,
                mkdirs,
                moves,
                chmods,
                touchzs,
                chgrps,
                javaOpts,
                args,
                nameNode,
                launcher,
                captureOutput);
    }

    /**
     * Create and return a new {@link ActionAttributesBuilder} that is based on an already built
     * {@link ActionAttributes} object. The properties of the builder will initially be the same as those of the
     * provided {@link ActionAttributes} object, but it is possible to modify them once.
     * @param attributes The {@link ActionAttributes} object on which this {@link ActionAttributesBuilder} will be based.
     * @return A new {@link ActionAttributesBuilder} that is based on a previously built
     *         {@link ActionAttributes} object.
     */
    public static ActionAttributesBuilder createFromExisting(final ActionAttributes attributes) {
        final ModifyOnce<String> resourceManager = new ModifyOnce<>(attributes.getResourceManager());
        final ModifyOnce<String> nameNode = new ModifyOnce<>(attributes.getNameNode());
        final ModifyOnce<Prepare> prepare = new ModifyOnce<>(attributes.getPrepare());
        final ModifyOnce<Streaming> streaming = new ModifyOnce<>(attributes.getStreaming());
        final ModifyOnce<Pipes> pipes = new ModifyOnce<>(attributes.getPipes());
        final List<String> jobXmls = new ArrayList<>(attributes.getJobXmls());
        final Map<String, ModifyOnce<String>> configuration = convertToModifyOnceMap(attributes.getConfiguration());
        final ModifyOnce<String> configClass = new ModifyOnce<>(attributes.getConfigClass());
        final List<String> files = new ArrayList<>(attributes.getFiles());
        final List<String> archives = new ArrayList<>(attributes.getArchives());
        final List<Delete> deletes = new ArrayList<>(attributes.getDeletes());
        final List<Mkdir> mkdirs = new ArrayList<>(attributes.getMkdirs());
        final List<Move> moves = new ArrayList<>(attributes.getMoves());
        final List<Chmod> chmods = new ArrayList<>(attributes.getChmods());
        final List<Touchz> touchzs = new ArrayList<>(attributes.getTouchzs());
        final List<Chgrp> chgrps = new ArrayList<>(attributes.getChgrps());
        final ModifyOnce<String> javaOpts = new ModifyOnce<>(attributes.getJavaOpts());
        final List<String> args = new ArrayList<>(attributes.getArgs());
        final ModifyOnce<Launcher> launcher = new ModifyOnce<>(attributes.getLauncher());
        final ModifyOnce<Boolean> captureOutput = new ModifyOnce<>(attributes.isCaptureOutput());

        return new ActionAttributesBuilder(
                resourceManager,
                prepare,
                streaming,
                pipes,
                jobXmls,
                configuration,
                configClass,
                files,
                archives,
                deletes,
                mkdirs,
                moves,
                chmods,
                touchzs,
                chgrps,
                javaOpts,
                args,
                nameNode,
                launcher,
                captureOutput);
    }

    public static ActionAttributesBuilder createFromAction(final Node action) {
        if (HasAttributes.class.isAssignableFrom(action.getClass()) && action instanceof HasAttributes) {
            return ActionAttributesBuilder.createFromExisting(((HasAttributes) action).getAttributes());
        }

        return ActionAttributesBuilder.create();
    }

    private ActionAttributesBuilder(final ModifyOnce<String> resourceManager,
                                    final ModifyOnce<Prepare> prepare,
                                    final ModifyOnce<Streaming> streaming,
                                    final ModifyOnce<Pipes> pipes,
                                    final List<String> jobXmls,
                                    final Map<String, ModifyOnce<String>> configuration,
                                    final ModifyOnce<String> configClass,
                                    final List<String> files,
                                    final List<String> archives,
                                    final List<Delete> deletes,
                                    final List<Mkdir> mkdirs,
                                    final List<Move> moves,
                                    final List<Chmod> chmods,
                                    final List<Touchz> touchzs,
                                    final List<Chgrp> chgrps,
                                    final ModifyOnce<String> javaOpts,
                                    final List<String> args,
                                    final ModifyOnce<String> nameNode,
                                    final ModifyOnce<Launcher> launcher,
                                    final ModifyOnce<Boolean> captureOutput) {
        this.nameNode = nameNode;
        this.prepare = prepare;
        this.streaming = streaming;
        this.pipes = pipes;
        this.jobXmls = jobXmls;
        this.configuration = configuration;
        this.configClass = configClass;
        this.files = files;
        this.archives = archives;
        this.deletes = deletes;
        this.mkdirs = mkdirs;
        this.moves = moves;
        this.chmods = chmods;
        this.touchzs = touchzs;
        this.chgrps = chgrps;
        this.javaOpts = javaOpts;
        this.args = args;
        this.resourceManager = resourceManager;
        this.launcher = launcher;
        this.captureOutput = captureOutput;
    }

    public void withResourceManager(final String resourceManager) {
        this.resourceManager.set(resourceManager);
    }

    /**
     * Registers a name node.
     * @param nameNode The string representing the name node.
     * @throws IllegalStateException if a name node has already been set on this builder.
     */
    public void withNameNode(final String nameNode) {
        this.nameNode.set(nameNode);
    }

    /**
     * Registers a {@link Prepare} object.
     * @param prepare The {@link Prepare} object to register.
     * @throws IllegalStateException if a {@link Prepare} object has already been set on this builder.
     */
    void withPrepare(final Prepare prepare) {
        this.prepare.set(prepare);
    }

    /**
     * Registers a {@link Streaming} object.
     * @param streaming The {@link Streaming} object to register.
     * @throws IllegalStateException if a {@link Streaming} object has already been set on this builder.
     */
    void withStreaming(final Streaming streaming) {
        this.streaming.set(streaming);
    }

    /**
     * Registers a {@link Pipes} object.
     * @param pipes The {@link Pipes} object to register.
     * @throws IllegalStateException if a {@link Pipes} object has already been set on this builder.
     */
    void withPipes(final Pipes pipes) {
        this.pipes.set(pipes);
    }

    /**
     * Registers a job XML with this builder.
     * @param jobXml The job XML to register.
     */
    public void withJobXml(final String jobXml) {
        this.jobXmls.add(jobXml);
    }

    /**
     * Removes a job XML if it is registered with this builder, otherwise does nothing.
     * @param jobXml The job XML to remove.
     */
    public void withoutJobXml(final String jobXml) {
        jobXmls.remove(jobXml);
    }

    /**
     * Removes all job XMLs that are registered with this builder.
     */
    public void clearJobXmls() {
        jobXmls.clear();
    }

    /**
     * Registers a configuration property (a key-value pair) with this builder. If the provided key has already been
     * set on this builder, an exception is thrown. Setting a key to null means deleting it.
     * @param key The name of the property to set.
     * @param value The value of the property to set.
     * @throws IllegalStateException if the provided key has already been set on this builder.
     */
    public void withConfigProperty(final String key, final String value) {
        ModifyOnce<String> mappedValue = this.configuration.get(key);

        if (mappedValue == null) {
            mappedValue = new ModifyOnce<>(value);
            this.configuration.put(key, mappedValue);
        }

        mappedValue.set(value);
    }

    /**
     * Registers a configuration class with this builder.
     * @param configClass The string representing the configuration class.
     * @throws IllegalStateException if a configuration class has already been set on this builder.
     */
    void withConfigClass(final String configClass) {
        this.configClass.set(configClass);
    }

    /**
     * Registers a file with this builder.
     * @param file The file to register.
     */
    void withFile(final String file) {
        this.files.add(file);
    }

    /**
     * Removes a file if it is registered with this builder, otherwise does nothing.
     * @param file The file to remove.
     */
    void withoutFile(final String file) {
        files.remove(file);
    }

    /**
     * Removes all files that are registered with this builder.
     */
    void clearFiles() {
        files.clear();
    }

    /**
     * Registers an archive with this builder.
     * @param archive The archive to register.
     */
    void withArchive(final String archive) {
        this.archives.add(archive);
    }

    /**
     * Removes an archive if it is registered with this builder, otherwise does nothing.
     * @param archive The archive to remove.
     */
    void withoutArchive(final String archive) {
        archives.remove(archive);
    }

    /**
     * Removes all archives that are registered with this builder.
     */
    void clearArchives() {
        archives.clear();
    }

    /**
     * Registers a {@link Delete} object with this builder.
     * @param delete The {@link Delete} object to register.
     */
    void withDelete(final Delete delete) {
        this.deletes.add(delete);
    }

    /**
     * Removes a {@link Delete} object if it is registered with this builder, otherwise does nothing.
     * @param delete The {@link Delete} object to remove.
     */
    void withoutDelete(final Delete delete) {
        deletes.remove(delete);
    }

    /**
     * Removes all {@link Delete} objects that are registered with this builder.
     */
    void clearDeletes() {
        deletes.clear();
    }

    /**
     * Registers a {@link Mkdir} object with this builder.
     * @param mkdir The {@link Mkdir} object to register.
     */
    void withMkdir(final Mkdir mkdir) {
        this.mkdirs.add(mkdir);
    }

    /**
     * Removes a {@link Mkdir} object if it is registered with this builder, otherwise does nothing.
     * @param mkdir The {@link Mkdir} object to remove.
     */
    void withoutMkdir(final Mkdir mkdir) {
        mkdirs.remove(mkdir);
    }

    /**
     * Removes all {@link Mkdir} objects that are registered with this builder.
     */
    void clearMkdirs() {
        mkdirs.clear();
    }

    /**
     * Registers a {@link Move} object with this builder.
     * @param move The {@link Move} object to register.
     */
    void withMove(final Move move) {
        this.moves.add(move);
    }

    /**
     * Removes a {@link Move} object if it is registered with this builder, otherwise does nothing.
     * @param move The {@link Move} object to remove.
     */
    void withoutMove(final Move move) {
        moves.remove(move);
    }

    /**
     * Removes all {@link Move} objects that are registered with this builder.
     */
    void clearMoves() {
        moves.clear();
    }

    /**
     * Registers a {@link Chmod} object with this builder.
     * @param chmod The {@link Chmod} object to register.
     */
    void withChmod(final Chmod chmod) {
        this.chmods.add(chmod);
    }

    /**
     * Removes a {@link Chmod} object if it is registered with this builder, otherwise does nothing.
     * @param chmod The {@link Chmod} object to remove.
     */
    void withoutChmod(final Chmod chmod) {
        chmods.remove(chmod);
    }

    /**
     * Removes all {@link Chmod} objects that are registered with this builder.
     */
    void clearChmods() {
        chmods.clear();
    }

    /**
     * Registers a {@link Touchz} object with this builder.
     * @param touchz The {@link Touchz} object to register.
     */
    void withTouchz(final Touchz touchz) {
        this.touchzs.add(touchz);
    }

    /**
     * Removes a {@link Touchz} object if it is registered with this builder, otherwise does nothing.
     * @param touchz The {@link Touchz} object to remove.
     */
    void withoutTouchz(final Touchz touchz) {
        touchzs.remove(touchz);
    }

    /**
     * Removes all {@link Touchz} objects that are registered with this builder.
     */
    void clearTouchzs() {
        touchzs.clear();
    }

    /**
     * Registers a {@link Chgrp} object with this builder.
     * @param chgrp The {@link Chgrp} object to register.
     */
    void withChgrp(final Chgrp chgrp) {
        this.chgrps.add(chgrp);
    }

    /**
     * Removes a {@link Chgrp} object if it is registered with this builder, otherwise does nothing.
     * @param chgrp The {@link Chgrp} object to remove.
     */
    void withoutChgrp(final Chgrp chgrp) {
        chgrps.remove(chgrp);
    }

    /**
     * Removes all {@link Chgrp} objects that are registered with this builder.
     */
    void clearChgrps() {
        chgrps.clear();
    }

    void withJavaOpts(final String javaOpts) {
        this.javaOpts.set(javaOpts);
    }

    void withArg(final String arg) {
        this.args.add(arg);
    }

    void withoutArg(final String arg) {
        this.args.remove(arg);
    }

    void clearArgs() {
        args.clear();
    }

    public void withLauncher(final Launcher launcher) {
        this.launcher.set(launcher);
    }

    void withCaptureOutput(final Boolean captureOutput) {
        this.captureOutput.set(captureOutput);
    }

    /**
     * Creates a new {@link ActionAttributes} object with the properties stores in this builder.
     * The new {@link ActionAttributes} object is independent of this builder and the builder can be used to build
     * new instances.
     * @return A new {@link ActionAttributes} object with the propertied stores in this builder.
     */
    public ActionAttributes build() {
        return new ActionAttributes(
                resourceManager.get(),
                nameNode.get(),
                prepare.get(),
                streaming.get(),
                pipes.get(),
                ImmutableList.copyOf(jobXmls),
                convertToConfigurationMap(configuration),
                configClass.get(),
                ImmutableList.copyOf(files),
                ImmutableList.copyOf(archives),
                ImmutableList.copyOf(deletes),
                ImmutableList.copyOf(mkdirs),
                ImmutableList.copyOf(moves),
                ImmutableList.copyOf(chmods),
                ImmutableList.copyOf(touchzs),
                ImmutableList.copyOf(chgrps),
                javaOpts.get(),
                ImmutableList.copyOf(args),
                launcher.get(),
                captureOutput.get());
    }

    static Map<String, ModifyOnce<String>> convertToModifyOnceMap(final Map<String, String> configurationMap) {
        final Map<String, ModifyOnce<String>> modifyOnceEntries = new LinkedHashMap<>();

        for (final Map.Entry<String, String> keyAndValue : configurationMap.entrySet()) {
            modifyOnceEntries.put(keyAndValue.getKey(), new ModifyOnce<>(keyAndValue.getValue()));
        }

        return modifyOnceEntries;
    }

    static ImmutableMap<String, String> convertToConfigurationMap(final Map<String, ModifyOnce<String>> map) {
        final Map<String, String> mutableConfiguration = new LinkedHashMap<>();

        for (final Map.Entry<String, ModifyOnce<String>> modifyOnceEntry : map.entrySet()) {
            if (modifyOnceEntry.getValue().get() != null) {
                mutableConfiguration.put(modifyOnceEntry.getKey(), modifyOnceEntry.getValue().get());
            }
        }

        return ImmutableMap.copyOf(mutableConfiguration);
    }
}
