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

import org.apache.hadoop.mapred.InputFormat;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.util.XLog;

class LauncherInputFormatClassLocator {
    private final XLog LOG = XLog.getLog(getClass());

    static final String HADOOP_INPUT_FORMAT_CLASSNAME = "mapreduce.input.format.class";

    private Class<? extends InputFormat> launcherInputFormatClass;

    /**
     * Get the {@code LauncherMapper} map only MR job's {@link org.apache.hadoop.mapreduce.InputFormat} according to configuration.
     * <p/>
     * Priority:
     * <ol>
     * <li><tt>oozie.action.launcher.mapreduce.input-format.class-name</tt></li>
     * <li><tt>OozieLauncherInputFormat</tt></li>
     * </ol>
     *
     * @return
     */
    Class<? extends InputFormat> locateOrGet() {
        if (launcherInputFormatClass == null) {
            launcherInputFormatClass = locate();
        }

        return launcherInputFormatClass;
    }

    /**
     * Get the {@code LauncherMapper} map only MR job's {@link org.apache.hadoop.mapreduce.InputFormat} according to configuration.
     * <p/>
     * Priority:
     * <ol>
     * <li><tt>oozie.action.launcher.mapreduce.input-format.class-name</tt></li>
     * <li><tt>OozieLauncherInputFormat</tt></li>
     * </ol>
     *
     * @return
     */
    private Class<? extends InputFormat> locate() {
        String inputFormatClassName = OozieLauncherInputFormat.class.getSimpleName();

        final String configuredClassName = ConfigurationService.get(
                JavaActionExecutor.OOZIE_ACTION_LAUNCHER_PREFIX + HADOOP_INPUT_FORMAT_CLASSNAME);
        if (configuredClassName != null) {
            inputFormatClassName = configuredClassName;
        }

        try {
            LOG.debug("Locating launcher input format class [{0}].", inputFormatClassName);

            final Class inputFormatClass = Class.forName(inputFormatClassName);
            LOG.debug("Launcher input format class [{0}] located successfully.", inputFormatClassName);

            return inputFormatClass;
        } catch (final ClassNotFoundException | ClassCastException e) {
            LOG.warn("Could not load class [{0}], located [{1}] instead.", inputFormatClassName,
                    OozieLauncherInputFormat.class.getSimpleName());
            return OozieLauncherInputFormat.class;
        }
    }
}
