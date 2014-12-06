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

package org.apache.oozie.sla.listener;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.event.BundleJobEvent;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.CoordinatorJobEvent;
import org.apache.oozie.event.WorkflowActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;
import org.apache.oozie.event.listener.JobEventListener;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.util.XLog;

public class SLAJobEventListener extends JobEventListener {

    @Override
    public void init(Configuration conf) {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void onWorkflowJobEvent(WorkflowJobEvent event) {
        sendEventToSLAService(event, event.getStatus().toString());
    }

    @Override
    public void onWorkflowActionEvent(WorkflowActionEvent event) {
        sendEventToSLAService(event, event.getStatus().toString());
    }

    @Override
    public void onCoordinatorJobEvent(CoordinatorJobEvent event) {
        sendEventToSLAService(event, event.getStatus().toString());
    }

    @Override
    public void onCoordinatorActionEvent(CoordinatorActionEvent event) {
        sendEventToSLAService(event, event.getStatus().toString());
    }

    @Override
    public void onBundleJobEvent(BundleJobEvent wje) {
    }

    private void sendEventToSLAService(JobEvent event, String status) {
        Date startTime = event.getStartTime();
        Date endTime = event.getEndTime();
        try {
            Services.get().get(SLAService.class)
                    .addStatusEvent(event.getId(), status, event.getEventStatus(), startTime, endTime);
        }
        catch (ServiceException se) {
            XLog.getLog(SLAService.class).error("Exception happened while sending Job-Status event for SLA", se);
        }
    }

}
