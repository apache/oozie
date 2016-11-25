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

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class YarnJobActions {
    private final Configuration configuration;
    private final ApplicationsRequestScope scope;
    private final boolean checkApplicationTags;
    private final boolean checkStartRange;

    private YarnJobActions(final Configuration configuration,
                           final ApplicationsRequestScope scope,
                           final boolean checkApplicationTags,
                           final boolean checkStartRange) {
        this.configuration = configuration;
        this.scope = scope;
        this.checkApplicationTags = checkApplicationTags;
        this.checkStartRange = checkStartRange;
    }

    public Set<ApplicationId> getYarnJobs() {
        System.out.println(String.format("Fetching yarn jobs. [scope=%s;checkApplicationTags=%s;checkStartRange=%s]",
                scope, checkApplicationTags, checkStartRange));

        final Set<ApplicationId> childYarnJobs = Sets.newHashSet();
        final GetApplicationsRequest gar = GetApplicationsRequest.newInstance();
        gar.setScope(scope);

        if (checkApplicationTags) {
            final String tag = configuration.get(LauncherMain.CHILD_MAPREDUCE_JOB_TAGS);
            if (tag == null) {
                System.out.println("Could not find Yarn tags property " + LauncherMain.CHILD_MAPREDUCE_JOB_TAGS);
                return childYarnJobs;
            }
            System.out.println("tag id : " + tag);
            gar.setApplicationTags(Collections.singleton(tag));
        }

        if (checkStartRange) {
            long startTime;
            try {
                startTime = Long.parseLong(System.getProperty(LauncherMain.OOZIE_JOB_LAUNCH_TIME));
            } catch (final NumberFormatException nfe) {
                throw new RuntimeException("Could not find Oozie job launch time", nfe);
            }


            long endTime = System.currentTimeMillis();
            if (startTime > endTime) {
                System.out.println("WARNING: Clock skew between the Oozie server host and this host detected.  Please fix this.  " +
                        "Attempting to work around...");
                // We don't know which one is wrong (relative to the RM), so to be safe, let's assume they're both wrong and add an
                // offset in both directions
                final long diff = 2 * (startTime - endTime);
                startTime = startTime - diff;
                endTime = endTime + diff;
            }
            gar.setStartRange(startTime, endTime);
        }

        try {
            final ApplicationClientProtocol proxy = ClientRMProxy.createRMProxy(configuration, ApplicationClientProtocol.class);
            final GetApplicationsResponse apps = proxy.getApplications(gar);
            final List<ApplicationReport> appsList = apps.getApplicationList();
            for (final ApplicationReport appReport : appsList) {
                childYarnJobs.add(appReport.getApplicationId());
            }
        } catch (final IOException | YarnException e) {
            throw new RuntimeException("Exception occurred while finding child jobs", e);
        }

        System.out.println("Child yarn jobs are found - " + StringUtils.join(childYarnJobs, ","));
        return childYarnJobs;
    }

    static void killChildYarnJobs(final Configuration actionConf) {
        final YarnJobActions yarnJobActions = new Builder(actionConf, ApplicationsRequestScope.OWN)
                .build();
        final Set<ApplicationId> childYarnJobs = yarnJobActions.getYarnJobs();

        yarnJobActions.killSelectedYarnJobs(childYarnJobs);
    }

    public void killSelectedYarnJobs(final Set<ApplicationId> selectedApplicationIds) {
        final YarnClient yarnClient = createYarnClient();

        try {
            if (!selectedApplicationIds.isEmpty()) {
                System.out.println("");
                System.out.println("Found [" + selectedApplicationIds.size() + "] Map-Reduce jobs from this launcher");
                System.out.println("Killing existing jobs and starting over:");

                for (final ApplicationId app : selectedApplicationIds) {
                    System.out.println("Killing job [" + app + "] ... ");

                    yarnClient.killApplication(app);

                    System.out.println("Done");
                }

                System.out.println("");
            }
        } catch (final YarnException | IOException e) {
            throw new RuntimeException("Exception occurred while killing child job(s)", e);
        } finally {
            Closeables.closeQuietly(yarnClient);
        }
    }

    private YarnClient createYarnClient() {
        final YarnClient yarnClient = YarnClient.createYarnClient();

        yarnClient.init(configuration);
        yarnClient.start();

        return yarnClient;
    }

    public static class Builder {
        private final Configuration configuration;
        private final ApplicationsRequestScope scope;
        private boolean checkApplicationTags = false;
        private boolean checkStartRange = false;

        public Builder(final Configuration configuration, final ApplicationsRequestScope scope) {
            this.configuration = configuration;
            this.scope = scope;
        }

        public Builder checkApplicationTags(final boolean checkApplicationTags) {
            this.checkApplicationTags = checkApplicationTags;

            return this;
        }

        public Builder checkStartRange(final boolean checkStartRange) {
            this.checkStartRange = checkStartRange;

            return this;
        }

        public YarnJobActions build() {
            return new YarnJobActions(configuration, scope, checkApplicationTags, checkStartRange);
        }
    }
}
