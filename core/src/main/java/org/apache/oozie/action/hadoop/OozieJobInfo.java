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
import java.io.StringReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.ActionExecutor.Context;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XConfiguration;

import com.google.common.annotations.VisibleForTesting;

public class OozieJobInfo {

    public static final String BUNDLE_ID = "bundle.id";
    public static final String BUNDLE_NAME = "bundle.name";
    public static final String COORD_NAME = "coord.name";
    public static final String COORD_ID = "coord.id";
    public static final String COORD_NOMINAL_TIME = "coord.nominal.time";
    public static final String WORKFLOW_ID = "wf.id";
    public static final String WORKFLOW_NAME = "wf.name";
    public static final String ACTION_TYPE = "action.type";
    public static final String ACTION_NAME = "action.name";
    public static final String JOB_INFO_KEY = "oozie.job.info";
    public static final String CONF_JOB_INFO = "oozie.action.jobinfo.enable";
    public final static String SEPARATOR = ",";

    private Context context;
    XConfiguration contextConf;
    private WorkflowAction action;
    private Configuration actionConf;
    private static boolean jobInfo = ConfigurationService.getBoolean(OozieJobInfo.CONF_JOB_INFO);

    /**
     * Instantiates a new oozie job info.
     *
     * @param actionConf the action conf
     * @param context the context
     * @param action the action
     * @throws IOException
     */
    public OozieJobInfo(Configuration actionConf, Context context, WorkflowAction action) throws IOException {
        this.context = context;
        contextConf = new XConfiguration(new StringReader(context.getWorkflow().getConf()));
        this.action = action;
        this.actionConf = actionConf;
    }

    public static boolean isJobInfoEnabled() {
        return jobInfo;
    }

    @VisibleForTesting
    public static void setJobInfo(boolean jobInfo) {
        OozieJobInfo.jobInfo = jobInfo;
    }

    /**
     * Get the job info.
     *
     * @return job info
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public String getJobInfo() throws IOException {
        StringBuffer sb = new StringBuffer();
        addBundleInfo(sb);
        addCoordInfo(sb);
        addWorkflowInfo(sb);
        addActionInfo(sb);
        addCustomInfo(sb);
        return sb.toString();

    }

    private void addBundleInfo(StringBuffer sb) throws IOException {
        addJobInfo(sb, BUNDLE_ID, contextConf.get(OozieClient.BUNDLE_ID));
        addJobInfo(sb, BUNDLE_NAME, contextConf.get(OozieJobInfo.BUNDLE_NAME));

    }

    private void addCoordInfo(StringBuffer sb) throws IOException {
        addJobInfo(sb, COORD_NAME, contextConf.get(OozieJobInfo.COORD_NAME));
        addJobInfo(sb, COORD_NOMINAL_TIME, contextConf.get(OozieJobInfo.COORD_NOMINAL_TIME));
        addJobInfo(sb, COORD_ID, context.getWorkflow().getParentId());

    }

    private void addWorkflowInfo(StringBuffer sb) {
        addJobInfo(sb, WORKFLOW_ID, context.getWorkflow().getId());
        addJobInfo(sb, WORKFLOW_NAME, context.getWorkflow().getAppName());

    }

    private void addActionInfo(StringBuffer sb) {
        addJobInfo(sb, ACTION_NAME, action.getName());
        addJobInfo(sb, ACTION_TYPE, action.getType());
    }

    private void addCustomInfo(StringBuffer sb) throws IOException {
        addfromConf(actionConf, sb);
    }

    public void addfromConf(Configuration conf, StringBuffer sb) {
        Iterator<Map.Entry<String, String>> it = conf.iterator();
        while (it.hasNext()) {
            Entry<String, String> entry = it.next();
            if (entry.getKey().startsWith("oozie.job.info.")) {
                addJobInfo(sb, entry.getKey().substring("oozie.job.info.".length()), entry.getValue());
            }
        }
    }

    private void addJobInfo(StringBuffer sb, String key, String value) {
        if (value != null) {
            sb.append(key).append("=").append(value).append(OozieJobInfo.SEPARATOR);
        }

    }
}
