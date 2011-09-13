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
package org.apache.oozie.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.Job;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StatusTransitService;

public class StatusUtils {

    /**
     * This Function transforms the statuses based on the name space of the coordinator App
     *
     * @param coordJob This will be the coordinator job bean for which we need to get the status based on version
     * @return Job.Status This would be the new status based on the app version.
     */
    public static Job.Status getStatus(CoordinatorJobBean coordJob) {
        Job.Status newStatus = null;
        if (coordJob != null) {
            newStatus = coordJob.getStatus();
            Configuration conf = Services.get().getConf();
            boolean backwardSupportForCoordStatus = conf.getBoolean(
                    StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, false);
            if (backwardSupportForCoordStatus) {
                if (coordJob.getAppNamespace() != null
                        && coordJob.getAppNamespace().equals(SchemaService.COORDINATOR_NAMESPACE_URI_1)) {

                    if (coordJob.getStatus() == Job.Status.DONEWITHERROR) {
                        newStatus = Job.Status.SUCCEEDED;
                    }
                    else if (coordJob.getStatus() == Job.Status.PAUSED) {
                        newStatus = Job.Status.RUNNING;
                    }
                    else if (coordJob.getStatus() == Job.Status.RUNNING && coordJob.isDoneMaterialization()) {
                        newStatus = Job.Status.SUCCEEDED;
                    }
                    else if (coordJob.getStatus() == Job.Status.PREPSUSPENDED) {
                        newStatus = Job.Status.SUSPENDED;
                    }
                    else if (coordJob.getStatus() == Job.Status.PREPPAUSED) {
                        newStatus = Job.Status.PREP;
                    }
                }
            }
        }
        return newStatus;
    }

    /**
     * This function changes back the status for coordinator rerun if the job was SUCCEEDED or SUSPENDED when rerun
     * with backward support is true.
     *
     * @param coordJob This will be the coordinator job bean for which we need to get the status based on version
     * @param prevStatus coordinator job previous status
     * @return Job.Status This would be the new status based on the app version.
     */
    public static Job.Status getStatusForCoordRerun(CoordinatorJobBean coordJob, Job.Status prevStatus) {
        Job.Status newStatus = null;
        if (coordJob != null) {
            newStatus = coordJob.getStatus();
            Configuration conf = Services.get().getConf();
            boolean backwardSupportForCoordStatus = conf.getBoolean(
                    StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, false);
            if (backwardSupportForCoordStatus) {
                if (coordJob.getAppNamespace() != null
                        && coordJob.getAppNamespace().equals(SchemaService.COORDINATOR_NAMESPACE_URI_1)) {

                    if (prevStatus == Job.Status.SUSPENDED) {
                        newStatus = Job.Status.SUSPENDED;
                    }
                    else if (coordJob.isDoneMaterialization() || prevStatus == Job.Status.SUCCEEDED) {
                        newStatus = Job.Status.SUCCEEDED;
                        coordJob.setDoneMaterialization();
                    }
                }
            }
        }
        return newStatus;
    }

    /**
     * This function check if eligible to do action input check  when running with backward support is true.
     *
     * @param coordJob This will be the coordinator job bean for which we need to get the status based on version
     * @return true if eligible to do action input check
     */
    public static boolean getStatusForCoordActionInputCheck(CoordinatorJobBean coordJob) {
        boolean ret = false;
        if (coordJob != null) {
            Configuration conf = Services.get().getConf();
            boolean backwardSupportForCoordStatus = conf.getBoolean(
                    StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, false);
            if (backwardSupportForCoordStatus) {
                if (coordJob.getAppNamespace() != null
                        && coordJob.getAppNamespace().equals(SchemaService.COORDINATOR_NAMESPACE_URI_1)) {

                    if (coordJob.getStatus() == Job.Status.SUCCEEDED) {
                        ret = true;
                    }
                    else if (coordJob.getStatus() == Job.Status.SUSPENDED) {
                        ret = true;
                    }
                }
            }
        }
        return ret;
    }

    /**
     * If namespace 0.1 is used and backward support is true, SUCCEEDED coord job can be killed
     *
     * @param coordJob the coordinator job
     * @return true if namespace 0.1 is used and backward support is true, SUCCEEDED coord job can be killed
     */
    public static boolean isV1CoordjobKillable(CoordinatorJobBean coordJob) {
        boolean ret = false;
        if (coordJob != null) {
            Configuration conf = Services.get().getConf();
            boolean backwardSupportForCoordStatus = conf.getBoolean(
                    StatusTransitService.CONF_BACKWARD_SUPPORT_FOR_COORD_STATUS, false);
            if (backwardSupportForCoordStatus) {
                if (coordJob.getAppNamespace() != null
                        && coordJob.getAppNamespace().equals(SchemaService.COORDINATOR_NAMESPACE_URI_1)) {
                    if (coordJob.getStatus() == Job.Status.SUCCEEDED) {
                        ret = true;
                    }
                }
            }
        }
        return ret;
    }
}
