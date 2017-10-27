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

package org.apache.oozie.action.hadoop;

import com.google.common.base.Strings;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.util.XLog;

import java.util.Collection;
import java.util.Map;

/**
 * Launcher AM's {@link Configuration} is injected using {@code <launcher>} element specific, YARN, MapReduce v2, and
 * MapReduce v1 specific override and prepend properties employing following rules:
 * <ul>
 *     <li>if {@code oozie.launcher.override} config property is {@code false}, original behavior will run: each and every parameter
 *     from {@link #sourceConfiguration} that begins with {@link #OOZIE_LAUNCHER_PREFIX} will be copied over to the target launcher
 *     {@code Configuration} with key without the prefix, and original value. Example input:
 *     {@code <launcher><vcores>1</vcores></launcher>}. Example output: {@code oozie.launcher.vcores=1} and {@code vcores=1}</li>
 *
 *     <li>or else, when properties are defined inside global or application specific {@code <launcher>} tags, those take
 *     precedence over the override parameters defined in the application section with {@link #OVERRIDE_PREFIX}. An example: inside
 *     {@code <launcher>} there was {@code oozie.launcher.vcores} defined, and also one of the parameters defined as values in
 *     {@code oozie-default.xml#oozie.launcher.override.vcores}, e.g. {@code yarn.app.mapreduce.am.resource.cpu-vcores} is present,
 *     the original value coming from &gt;launcher&lt; tag will be used and copied over as {@code oozie.launcher.vcores}. Example
 *     input: {@code <launcher><vcores>1</vcores></launcher>}, and {@code yarn.app.mapreduce.am.resource.cpu-vcores=2} are both set.
 *     Example output: {@code oozie.launcher.vcores=1}</li>
 *
 *     <li>or else, when no properties are defined inside global or application specific {@code <launcher>} tags, there is a
 *     precedence ordering among the override parameters defined in the application section with {@link #OVERRIDE_PREFIX}. An
 *     example: {@code oozie.launcher.vcores} is not defined inside {@code <launcher>}, but there are both
 *     {@code oozie.launcher.override.vcores} properties {@code yarn.app.mapreduce.am.resource.cpu-vcores} and
 *     {@code mapreduce.map.cpu.vcores} present: the value of {@code yarn.app.mapreduce.am.resource.cpu-vcores} will be used as
 *     {@code oozie.launcher.vcores} inside target Launcher AM {@code Configuration}. Example input:
 *     {@code yarn.app.mapreduce.am.resource.cpu-vcores=1} and  {@code mapreduce.map.cpu.vcores=2}. Example output:
 *     {@code oozie.launcher.vcores=1}</li>
 *
 *     <li>or else, no properties are defined inside global or application specific {@code <launcher>} tags, nor there are any
 *     override parameters. In that case, {@code oozie.launcher.vcores} inside target Launcher AM {@code Configuration} is not
 *     filled. Example input: nothing. Example output: nothing</li>
 *
 *     <li>when prepending properties (the ones pointing to keys beginning with {@link #PREPEND_PREFIX}) are defined, these are
 *     prepended to the launcher settings coming from other sources. If no launcher settings of the available
 *     {@code #PREPEND_PREFIX} are defined, there is nothing prepended. Example input: {@code <launcher><env>B=BB</env></launcher>}
 *     and {@code oozie.launcher.prepend.env="A=AA"}. Example output: {@code oozie.launcher.env="A=AA B=BB"}</li>
 * </ul>
 */
class LauncherConfigurationInjector {
    private static final XLog LOG = XLog.getLog(LauncherConfigurationInjector.class);

    private static final String OOZIE_LAUNCHER_PREFIX = "oozie.launcher.";
    private static final String OVERRIDE_PREFIX = OOZIE_LAUNCHER_PREFIX + "override.";
    private static final String PREPEND_PREFIX = OOZIE_LAUNCHER_PREFIX + "prepend.";

    /**
     * All the Launcher AM override property keys as configured in {@code oozie-default.xml} beginning with
     * {@link #OVERRIDE_PREFIX}. Important to use {@link LinkedHashMultimap}, as we want an unified set of values those insertion /
     * configuration order is preserved.
     * <p/>
     * By the time it's initialized, it will have content like that:
     * <pre>
     * {@code
     * {oozie.launcher.override.priority=
     *          [mapreduce.job.priority, mapred.job.priority],
     *  oozie.launcher.override.queue=
     *          [mapreduce.job.queuename, mapred.job.queue.name],
     *  oozie.launcher.override.javaopts=
     *          [yarn.app.mapreduce.am.command-opts, mapreduce.map.java.opts, mapred.child.java.opts],
     *  oozie.launcher.override.log.level=
     *          [mapreduce.map.log.level, mapred.map.child.log.level],
     *  oozie.launcher.override.max.attempts=
     *          [mapreduce.map.maxattempts, mapred.map.max.attempts],
     *  oozie.launcher.override.vcores=
     *          [yarn.app.mapreduce.am.resource.cpu-vcores, mapreduce.map.cpu.vcores],
     *  oozie.launcher.override.memory.mb=
     *          [yarn.app.mapreduce.am.resource.mb, mapreduce.map.memory.mb, mapred.job.map.memory.mb],
     *  oozie.launcher.override.env=
     *          [yarn.app.mapreduce.am.env, mapreduce.map.env, mapred.child.env]}
     * }
     * </pre>
     */
    private static final Multimap<String, String> CONFIGURED_OVERRIDE_PROPERTIES = LinkedHashMultimap.create();

    /**
     * All the Launcher AM prepend property keys as configured in {@code oozie-default.xml} beginning with {@link #PREPEND_PREFIX}.
     * Important to use {@link LinkedHashMultimap}, as we want an unified set of values those insertion / configuration order is
     * preserved.
     * <p/>
     * By the time it's initialized, it will have content like that:
     * <pre>
     * {@code
     * {oozie.launcher.prepend.javaopts=
     *      [yarn.app.mapreduce.am.admin-command-opts],
     *  oozie.launcher.prepend.env=
     *      [yarn.app.mapreduce.am.admin.user.env]}
     * }
     * </pre>
     */
    private static final Multimap<String, String> CONFIGURED_PREPEND_PROPERTIES = LinkedHashMultimap.create();

    /**
     * Matches everything beginning with {@code oozie.launcher.override.}
     */
    private static final String OVERRIDE_REGEX = "^oozie\\.launcher\\.override\\.+";

    /**
     * Matches everything beginning with {@code oozie.launcher.prepend.}
     */
    private static final String PREPEND_REGEX = "^oozie\\.launcher\\.prepend\\.+";

    static {
        fillConfigPropertiesByRegex(OVERRIDE_REGEX, CONFIGURED_OVERRIDE_PROPERTIES);
        fillConfigPropertiesByRegex(PREPEND_REGEX, CONFIGURED_PREPEND_PROPERTIES);
    }

    /**
     * Fill in the target {@code Multimap} based like this: keys are extracted from {@link ConfigurationService} using {@code regex}
     * as prefix, values are the ones under the same key inside {@code oozie-[default,site].xml}.
     * <p/>
     * Depending on what {@code Multimap} we use, the values might keep config / insert order, and filter duplicates. At the end of
     * this call {@code target} will have all the key / value mappings enlisted in the Oozie configuration files
     * {@code oozie-[default,site].xml} those keys match {@code regex}.
     * <p/>
     * An example:
     * {@code oozie-default.xml} snippet:
     * <pre>
     * {@code
     *         <property>
     *             <name>oozie.launcher.override.max.attempts</name>
     *             <value>mapreduce.map.maxattempts,mapred.map.max.attempts</value>
     *         </property>
     * }
     * </pre>
     * results in:
     * <pre>
     * {@code
     *  {oozie.launcher.override.max.attempts=
     *          [mapreduce.map.maxattempts, mapred.map.max.attempts]}
     * }
     * </pre>
     * @param regex regular expression to match the keys, e.g. {@link #OVERRIDE_REGEX}
     * @param target a {@code Multimap<String, String>} as target, e.g. {@link #CONFIGURED_OVERRIDE_PROPERTIES}
     */
    private static void fillConfigPropertiesByRegex(final String regex, final Multimap<String, String> target) {
        for (final Map.Entry<String, String> overrideEntry : ConfigurationService.getValByRegex(regex).entrySet()) {
            final String key = overrideEntry.getKey();
            for (final String value : ConfigurationService.getStrings(key)) {
                target.put(key, value);
            }
        }
    }

    /**
     * The source {@code Configuration} provided by the caller
     */
    private final Configuration sourceConfiguration;

    /**
     * Override values that are actually present in {@link #sourceConfiguration}
     */
    private final Map<String, String> overridesPresentInSource = Maps.newLinkedHashMap();

    /**
     * Prepend values that are actually present in {@link #sourceConfiguration}
     */
    private final Map<String, String> prependsPresentInSource = Maps.newLinkedHashMap();

    LauncherConfigurationInjector(final Configuration sourceConfiguration) {
        this.sourceConfiguration = sourceConfiguration;
    }

    /**
     * Inject the overridden and prepended values from {@link #sourceConfiguration} into {@code launcherConf} wherever applicable.
     * @param launcherConf the target {@code Configuration} of the Launcher AM
     */
    void inject(final Configuration launcherConf) {
        LOG.debug("Injecting configuration entries to launcher configuration.");

        copyToLauncherConf(sourceConfiguration, launcherConf);

        LOG.debug("Configuration entries copied to launcher configuration successfully.");

        if (ConfigurationService.getBoolean("oozie.launcher.override")) {
            LOG.debug("Overriding and prepending launcher configuration.");
            LOG.trace("Filling override and prepend configuration values.");

            fillOverridesOrPrepends(CONFIGURED_OVERRIDE_PROPERTIES, OVERRIDE_PREFIX, overridesPresentInSource);
            fillOverridesOrPrepends(CONFIGURED_PREPEND_PROPERTIES, PREPEND_PREFIX, prependsPresentInSource);

            LOG.trace("Override and prepend configuration values filled. " +
                            "[overridesPresentInSource={0};prependsPresentInSource={1}]",
                    overridesPresentInSource,
                    prependsPresentInSource);

            overrideAndPrependLauncherConf(launcherConf);

            LOG.debug("Launcher configuration overridden and prepended.");
            LOG.trace("Launcher configuration finalized. [launcherConf={0}]", launcherConf);
        }

        LOG.debug("Configuration entries injected to launcher configuration.");
    }

    /**
     * Fill override / prepend state, based on preconfigured values.
     * <p/>
     * An example. Having inputs (no launcher tag is present, and only MapReduce v1 specific parameter is there):
     * <pre>
     *     {@code
     *     this.sourceConfiguration={mapred.map.max.attempts=2}
     *     overrideOrPrependProperties={oozie.launcher.override.max.attempts=[mapreduce.map.maxattempts, mapred.map.max.attempts]}
     *     prefix="oozie.launcher.override."
     *     target={}
     *     }
     * </pre>
     * results in:
     * <pre>
     *     {@code
     *     target={oozie.launcher.max.attempts=2}
     *     }
     * </pre>
     * thus, the MapReduce v1 specific parameter is applied to the specific Launcher AM {@code Configuration}.
     * <p/>
     * Note that defined {@code oozie.launcher.override.*} properties will have an output substituting / adding an config entry
     * {@code oozie.launcher.*}, while {@code oozie.launcher.prepend.*} entries will be prepended to existing
     * {@code oozie.launcher.*} entries (or to new ones if {@code oozie.launcher.override.*} is present also).
     * @param overrideOrPrependProperties either {@link #CONFIGURED_OVERRIDE_PROPERTIES} or {@link #CONFIGURED_PREPEND_PROPERTIES}
     * @param prefix either {@link #OVERRIDE_PREFIX} or {@link #PREPEND_PREFIX}
     * @param target a {@code Map<String, String>} consisting of configured override or prepend values as present in the source
     */
    private void fillOverridesOrPrepends(final Multimap<String, String> overrideOrPrependProperties,
                                         final String prefix,
                                         final Map<String, String> target) {
        for (final Map.Entry<String, Collection<String>> overrideProperty : overrideOrPrependProperties.asMap().entrySet()) {
            final String launcherKey = overrideProperty.getKey().replace(prefix, OOZIE_LAUNCHER_PREFIX);
            final Collection<String> sourceKeys = overrideProperty.getValue();
            LOG.trace("Filling launcher override / prepend value. [sourceKeys={0};launcherKey={1}]", sourceKeys, launcherKey);

            fillOverrideOrPrependLauncherValue(sourceKeys, launcherKey, target);
        }
    }

    /**
     * Fill one piece of override / prepend state, based on preconfigured value.
     * @param sourceKeys
     * @param launcherKey
     * @param target
     */
    private void fillOverrideOrPrependLauncherValue(final Collection<String> sourceKeys,
                                                    final String launcherKey,
                                                    final Map<String, String> target) {
        final String sourceValue = getFirstSourceValue(sourceKeys);
        if (!Strings.isNullOrEmpty(sourceValue)) {
            target.put(launcherKey, sourceValue);
        }
    }

    /**
     * Get the first non-empty value present in {@link #sourceConfiguration} given one of {@code propertyKeys}, order-sensitively.
     * @param propertyKeys
     * @return
     */
    private String getFirstSourceValue(final Collection<String> propertyKeys) {
        if (propertyKeys == null) {
            return null;
        }

        for (final String propertyKey : propertyKeys) {
            final String propertyValue = sourceConfiguration.get(propertyKey);
            if (!Strings.isNullOrEmpty(propertyValue)) {
                return propertyValue;
            }
        }

        return null;
    }

    /**
     * Based on the override / prepend state, modify {@code launcherConfiguration}.
     * @param launcherConfiguration the target {@code Configuration} of the Launcher AM
     */
    private void overrideAndPrependLauncherConf(final Configuration launcherConfiguration) {
        for (final String overrideKey : overridesPresentInSource.keySet()) {
            overrideSingleValue(launcherConfiguration, overrideKey);
        }

        for (final String prependKey : prependsPresentInSource.keySet()) {
            prependSingleValue(launcherConfiguration, prependKey);
        }
    }

    /**
     * Based on one piece of override state, override a single entry in {@code launcherConfiguration}.
     * @param launcherConfiguration the target {@code Configuration} of the Launcher AM
     * @param key the override key
     */
    private void overrideSingleValue(final Configuration launcherConfiguration, final String key) {
        final String originalLauncherValue = launcherConfiguration.get(key);
        final String overrideValue = overridesPresentInSource.get(key);
        if (Strings.isNullOrEmpty(originalLauncherValue) && overrideValue != null) {
            launcherConfiguration.set(key, overrideValue);
        }
    }

    /**
     * Based on one piece of prepend state, prepend a single entry in {@code launcherConfiguration}.
     * @param launcherConfiguration the target {@code Configuration} of the Launcher AM
     * @param key the prepend key
     */
    private void prependSingleValue(final Configuration launcherConfiguration, final String key) {
        final String launcherValue = launcherConfiguration.get(key);
        final String prependValue = prependsPresentInSource.get(key);
        if (!Strings.isNullOrEmpty(prependValue)) {
            if (Strings.isNullOrEmpty(launcherValue)) {
                launcherConfiguration.set(key, prependValue);
            } else {
                launcherConfiguration.set(key, prependValue + " " + launcherValue);
            }
        }
    }

    /**
     * Copy each and every configuration entry between {@code source} and {@code target} beginning with
     * {@link #OOZIE_LAUNCHER_PREFIX}. Note that both original and un-prefixed keys will be present with the original value.
     * @param source
     * @param target
     */
    private void copyToLauncherConf(final Configuration source, final Configuration target) {
        for (final Map.Entry<String, String> entry : source) {
            if (entry.getKey().startsWith(OOZIE_LAUNCHER_PREFIX)) {
                final String name = entry.getKey().substring(OOZIE_LAUNCHER_PREFIX.length());
                final String value = entry.getValue();
                // setting original KEY
                target.set(entry.getKey(), value);
                // setting un-prefixed key to allow Hadoop job config for the launcher job
                target.set(name, value);
            }
        }
    }
}