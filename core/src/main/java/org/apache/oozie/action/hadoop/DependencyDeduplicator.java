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
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.XLog;

public class DependencyDeduplicator {
    public static final XLog LOG = XLog.getLog(DependencyDeduplicator.class);

    private static final String SYMLINK_SEPARATOR = "#";
    private static final String DEPENDENCY_SEPARATOR = ",";

    public void deduplicate(final Configuration conf, final String key) {
        if(conf == null || key == null) {
            return;
        }

        final String commaSeparatedFilePaths = conf.get(key);
        LOG.debug("Oozie tries to deduplicate dependencies with key [{0}], which has value of [{1}]",
                key,
                commaSeparatedFilePaths);

        if(Strings.isNullOrEmpty(commaSeparatedFilePaths)) {
            return;
        }

        final Map<String, String> nameToPath = new HashMap<>();
        final StringBuilder uniqList = new StringBuilder();

        final String[] dependencyPaths = commaSeparatedFilePaths.split(DEPENDENCY_SEPARATOR);
        for (String dependencyPath : dependencyPaths) {
            try {
                final String dependencyName = resolveName(
                        dependencyPath.substring(
                                dependencyPath.lastIndexOf(File.separator) + DEPENDENCY_SEPARATOR.length()
                        )
                );

                if (nameToPath.putIfAbsent(dependencyName, dependencyPath) == null) {
                    uniqList.append(dependencyPath).append(DEPENDENCY_SEPARATOR);
                } else {
                    LOG.warn("{0}[{1}] is already defined in {2}. Skipping.",
                            dependencyName, dependencyPath, key);
                }
            } catch (final IndexOutOfBoundsException e) {
                LOG.warn("Dependency [{0}] is malformed. Skipping.", dependencyPath);
            }
        }
        if(uniqList.length() > 0) {
            uniqList.setLength(uniqList.length() - 1);
        }
        conf.set(key, uniqList.toString());
        LOG.info("{0} dependencies are unified by their filename or symlink name.", key);
    }

    private String resolveName(final String dependencyName) {
        if(dependencyName.contains(SYMLINK_SEPARATOR)) {
            return dependencyName.split(SYMLINK_SEPARATOR)[1];
        }
        return dependencyName;
    }
}
