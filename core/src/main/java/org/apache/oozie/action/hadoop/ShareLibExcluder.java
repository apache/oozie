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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.XLog;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.apache.oozie.action.hadoop.JavaActionExecutor.ACTION_SHARELIB_FOR;
import static org.apache.oozie.action.hadoop.JavaActionExecutor.SHARELIB_EXCLUDE_SUFFIX;

class ShareLibExcluder {

    private final URI shareLibRoot;
    private final Pattern configuredExcludePattern;
    private static XLog LOG = XLog.getLog(ShareLibExcluder.class);
    @VisibleForTesting
    static final String VALUE_NULL_MSG = "The value of %s cannot be null.";

    ShareLibExcluder(final Configuration actionConf, final Configuration servicesConf, final Configuration jobConf,
                     final String executorType, final URI shareLibRoot) {
        this.shareLibRoot = shareLibRoot;
        this.configuredExcludePattern = loadAndBuildPattern(actionConf, servicesConf, jobConf, executorType);
    }

    private Pattern loadAndBuildPattern(final Configuration actionConf, final Configuration servicesConf,
                                        final Configuration jobConf, final String executorType) {
        if(shareLibRoot == null) {
            return null;
        }

        Objects.requireNonNull(actionConf, () -> String.format(VALUE_NULL_MSG, "actionConf"));
        Objects.requireNonNull(servicesConf, () -> String.format(VALUE_NULL_MSG, "servicesConf"));
        Objects.requireNonNull(jobConf, () -> String.format(VALUE_NULL_MSG, "jobConf"));

        final String excludeProperty = ACTION_SHARELIB_FOR + executorType + SHARELIB_EXCLUDE_SUFFIX;

        final Optional<String> maybeExcludePattern = tryFindExcludePattern(actionConf, servicesConf, jobConf, excludeProperty);
        if (maybeExcludePattern.isPresent()) {
            final String excludePattern = maybeExcludePattern.get();
            actionConf.set(excludeProperty, excludePattern);
            LOG.debug("Setting action configuration property: {0}={1}", excludeProperty, excludePattern);
            LOG.info("The following sharelib exclude pattern will be used: {0}", excludePattern);

            return Pattern.compile(excludePattern);
        }
        return null;
    }

    private Optional<String> tryFindExcludePattern(final Configuration actionConf, final Configuration servicesConf,
                                                   final Configuration jobConf, final String excludeProperty) {
        String excludePattern = actionConf.get(excludeProperty);

        if (excludePattern == null) {
            excludePattern = jobConf.get(excludeProperty);
        }

        if (excludePattern == null) {
            excludePattern = servicesConf.get(excludeProperty);
        }

        if (excludePattern == null) {
            LOG.info("Sharelib exclude pattern not configured, skipping.");
            return Optional.empty();
        }
        return Optional.of(excludePattern);
    }

    boolean shouldExclude(final URI actionLibURI) {
        Objects.requireNonNull(actionLibURI, () -> String.format(VALUE_NULL_MSG, "actionLibURI"));

        if (configuredExcludePattern != null && shareLibRoot != null) {
            if (configuredExcludePattern.matcher(shareLibRoot.relativize(actionLibURI).getPath()).matches()) {
                LOG.info("Mark file for excluding from distributed cache: {0}", actionLibURI.getPath());
                return true;
            }
        }
        LOG.debug("Mark file for adding to distributed cache: {0}", actionLibURI.getPath());
        return false;
    }
}
