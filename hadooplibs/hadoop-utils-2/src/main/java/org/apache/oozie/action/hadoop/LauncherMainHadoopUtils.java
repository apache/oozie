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
        Set<ApplicationId> childYarnJobs = new HashSet<ApplicationId>();
        String tag = actionConf.get(CHILD_MAPREDUCE_JOB_TAGS);
        if (tag == null) {
            System.out.print("Could not find Yarn tags property " + CHILD_MAPREDUCE_JOB_TAGS);
            return childYarnJobs;
        }
        System.out.println("tag id : " + tag);
        long startTime = 0L;
        try {
            if(actionConf.get(OOZIE_JOB_LAUNCH_TIME) != null) {
                startTime = Long.parseLong(actionConf.get(OOZIE_JOB_LAUNCH_TIME));
            }
            else {
                startTime = Long.parseLong(System.getProperty(OOZIE_JOB_LAUNCH_TIME));
            }
        } catch(NumberFormatException nfe) {
            throw new RuntimeException("Could not find Oozie job launch time", nfe);
        }

        GetApplicationsRequest gar = GetApplicationsRequest.newInstance();
        gar.setScope(ApplicationsRequestScope.OWN);
        gar.setApplicationTags(Collections.singleton(tag));
        long endTime = System.currentTimeMillis();
        if (startTime > endTime) {
            System.out.println("WARNING: Clock skew between the Oozie server host and this host detected.  Please fix this.  " +
                    "Attempting to work around...");
            // We don't know which one is wrong (relative to the RM), so to be safe, let's assume they're both wrong and add an
            // offset in both directions
            long diff = 2 * (startTime - endTime);
            startTime = startTime - diff;
            endTime = endTime + diff;
        }
        gar.setStartRange(startTime, endTime);
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

    public static Set<String> getChildJobs(Configuration actionConf) {
        Set<String> jobList = new HashSet<String>();
        for(ApplicationId applicationId :getChildYarnJobs(actionConf)) {
            jobList.add(applicationId.toString().replace("application", "job"));
        }
        return jobList;
    }
}
