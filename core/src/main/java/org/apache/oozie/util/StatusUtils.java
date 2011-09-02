/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
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
}