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

import org.apache.oozie.AppType;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.event.Event;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.XLogService;

/**
 * logging utilities.
 */
public class LogUtils {

    /**
     * Set the log info with the context of the given coordinator bean.
     *
     * @param cBean coordinator bean.
     * @param logInfo log info
     */
    public static void setLogInfo(CoordinatorJobBean cBean, XLog.Info logInfo) {
        if (logInfo.getParameter(XLogService.GROUP) == null) {
            logInfo.setParameter(XLogService.GROUP, cBean.getGroup());
        }
        if (logInfo.getParameter(XLogService.USER) == null) {
            logInfo.setParameter(XLogService.USER, cBean.getUser());
        }
        logInfo.setParameter(DagXLogInfoService.JOB, cBean.getId());
        logInfo.setParameter(DagXLogInfoService.TOKEN, "");
        logInfo.setParameter(DagXLogInfoService.APP, cBean.getAppName());
        XLog.Info.get().setParameters(logInfo);
    }

    /**
     * Set the log info with the context of the given coordinator action bean.
     *
     * @param action action bean.
     * @param logInfo log info
     */
    public static void setLogInfo(CoordinatorActionBean action, XLog.Info logInfo) {
        logInfo.setParameter(DagXLogInfoService.JOB, action.getJobId());
        logInfo.setParameter(DagXLogInfoService.ACTION, action.getId());
        XLog.Info.get().setParameters(logInfo);
    }

    /**
     * Set the log info with the context of the given workflow bean.
     *
     * @param workflow workflow bean.
     * @param logInfo log info
     */
    public static void setLogInfo(WorkflowJobBean workflow, XLog.Info logInfo) {
        logInfo.setParameter(XLogService.GROUP, workflow.getGroup());
        logInfo.setParameter(XLogService.USER, workflow.getUser());
        logInfo.setParameter(DagXLogInfoService.JOB, workflow.getId());
        logInfo.setParameter(DagXLogInfoService.TOKEN, workflow.getLogToken());
        logInfo.setParameter(DagXLogInfoService.APP, workflow.getAppName());
        XLog.Info.get().setParameters(logInfo);
    }

    /**
     * Set the log info with the context of the given action bean.
     *
     * @param action action bean.
     * @param logInfo log info
     */
    public static void setLogInfo(WorkflowActionBean action, XLog.Info logInfo) {
        logInfo.setParameter(DagXLogInfoService.JOB, action.getJobId());
        logInfo.setParameter(DagXLogInfoService.TOKEN, action.getLogToken());
        logInfo.setParameter(DagXLogInfoService.ACTION, action.getId());
        XLog.Info.get().setParameters(logInfo);
    }

    public static void setLogInfo(WorkflowAction action, XLog.Info logInfo) {
        String actionId = action.getId();
        logInfo.setParameter(DagXLogInfoService.JOB, actionId.substring(0, actionId.indexOf("@")));
        logInfo.setParameter(DagXLogInfoService.ACTION, actionId);
        XLog.Info.get().setParameters(logInfo);
    }

    /**
     * Set the log info with the context of the given bundle bean.
     *
     * @param bBean bundle bean.
     * @param logInfo log info
     */
    public static void setLogInfo(BundleJobBean bBean, XLog.Info logInfo) {
        if (logInfo.getParameter(XLogService.GROUP) == null) {
            logInfo.setParameter(XLogService.GROUP, bBean.getGroup());
        }
        if (logInfo.getParameter(XLogService.USER) == null) {
            logInfo.setParameter(XLogService.USER, bBean.getUser());
        }
        logInfo.setParameter(DagXLogInfoService.JOB, bBean.getId());
        logInfo.setParameter(DagXLogInfoService.TOKEN, "");
        logInfo.setParameter(DagXLogInfoService.APP, bBean.getAppName());
        XLog.Info.get().setParameters(logInfo);
    }

    /**
     * Set the thread local log info with the context of the given Info object.
     *
     * @param logInfo log info
     */
    public static void setLogInfo(XLog.Info logInfo) {
        XLog.Info.get().setParameters(logInfo);
    }

    public static XLog setLogInfo(XLog logObj, String jobId, String actionId, String appName) {
        clearLogPrefix();
        XLog.Info logInfo = XLog.Info.get();
        logInfo.setParameter(DagXLogInfoService.JOB, jobId);
        if (actionId != null) {
            logInfo.setParameter(DagXLogInfoService.ACTION, actionId);
        }
        if (appName != null) {
            logInfo.setParameter(DagXLogInfoService.APP, appName);
        }
        return XLog.resetPrefix(logObj);
    }

    public static XLog setLogPrefix(XLog logObj, Event event) {
        String jobId = null, actionId = null, appName = null;
        if (event instanceof JobEvent) {
            JobEvent je = (JobEvent) event;
            if (je.getAppType() == AppType.WORKFLOW_JOB || je.getAppType() == AppType.COORDINATOR_JOB
                    || je.getAppType() == AppType.BUNDLE_JOB) {
                jobId = je.getId();
            }
            else {
                actionId = je.getId();
                jobId = Services.get().get(UUIDService.class).getId(actionId);
            }
            appName = je.getAppName();
        }
        else if (event instanceof SLAEvent) {
            SLAEvent se = (SLAEvent) event;
            if (se.getAppType() == AppType.WORKFLOW_JOB || se.getAppType() == AppType.COORDINATOR_JOB
                    || se.getAppType() == AppType.BUNDLE_JOB) {
                jobId = se.getId();
            }
            else {
                actionId = se.getId();
                jobId = Services.get().get(UUIDService.class).getId(actionId);
            }
            appName = se.getAppName();
        }
        return LogUtils.setLogInfo(logObj, jobId, actionId, appName);
    }

    public static void clearLogPrefix() {
        XLog.Info.get().clearParameter(DagXLogInfoService.JOB);
        XLog.Info.get().clearParameter(DagXLogInfoService.ACTION);
        XLog.Info.get().clearParameter(DagXLogInfoService.APP);
    }

}
