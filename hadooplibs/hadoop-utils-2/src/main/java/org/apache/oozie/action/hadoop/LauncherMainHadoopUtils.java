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

import java.io.IOException;
import java.lang.String;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

public class LauncherMainHadoopUtils {

    public static final String CHILD_MAPREDUCE_JOB_TAGS = "oozie.child.mapreduce.job.tags";
    public static final String OOZIE_JOB_LAUNCH_TIME = "oozie.job.launch.time";

    private LauncherMainHadoopUtils() {
    }

    private static Set<ApplicationId> getChildYarnJobs(Configuration actionConf) {
        System.out.println("Fetching child yarn jobs");
        long startTime = 0L;
        try {
            startTime = Long.parseLong(System.getProperty(OOZIE_JOB_LAUNCH_TIME));
        } catch(NumberFormatException nfe) {
            throw new RuntimeException("Could not find Oozie job launch time", nfe);
        }

        Set<ApplicationId> childYarnJobs = new HashSet<ApplicationId>();
        if (actionConf.get(CHILD_MAPREDUCE_JOB_TAGS) == null) {
            System.out.print("Could not find Yarn tags property " + CHILD_MAPREDUCE_JOB_TAGS);
            return childYarnJobs;
        }

        String tag = actionConf.get(CHILD_MAPREDUCE_JOB_TAGS);
        System.out.println("tag id : " + tag);
        GetApplicationsRequest gar = GetApplicationsRequest.newInstance();
        gar.setScope(ApplicationsRequestScope.OWN);
        gar.setApplicationTags(Collections.singleton(tag));
        gar.setStartRange(startTime, System.currentTimeMillis());
        try {
            ApplicationClientProtocol proxy = ClientRMProxy.createRMProxy(actionConf, ApplicationClientProtocol.class);
            GetApplicationsResponse apps = proxy.getApplications(gar);
            List<ApplicationReport> appsList = apps.getApplicationList();
            for(ApplicationReport appReport : appsList) {
                childYarnJobs.add(appReport.getApplicationId());
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Exception occurred while finding child jobs", ioe);
        } catch (YarnException ye) {
            throw new RuntimeException("Exception occurred while finding child jobs", ye);
        }

        System.out.println("Child yarn jobs are found - " + StringUtils.join(childYarnJobs, ","));
        return childYarnJobs;
    }

    public static void killChildYarnJobs(Configuration actionConf) {
        try {
            Set<ApplicationId> childYarnJobs = getChildYarnJobs(actionConf);
            if (!childYarnJobs.isEmpty()) {
                System.out.println();
                System.out.println("Found [" + childYarnJobs.size() + "] Map-Reduce jobs from this launcher");
                System.out.println("Killing existing jobs and starting over:");
                YarnClient yarnClient = YarnClient.createYarnClient();
                yarnClient.init(actionConf);
                yarnClient.start();
                for (ApplicationId app : childYarnJobs) {
                    System.out.print("Killing job [" + app + "] ... ");
                    yarnClient.killApplication(app);
                    System.out.println("Done");
                }
                System.out.println();
            }
        } catch (YarnException ye) {
            throw new RuntimeException("Exception occurred while killing child job(s)", ye);
        } catch (IOException ioe) {
            throw new RuntimeException("Exception occurred while killing child job(s)", ioe);
        }
    }
}
