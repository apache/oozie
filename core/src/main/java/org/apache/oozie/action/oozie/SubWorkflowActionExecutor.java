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
package org.apache.oozie.action.oozie;

import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.DagEngine;
import org.apache.oozie.LocalOozieClient;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.service.Services;
import org.apache.hadoop.conf.Configuration;
import org.jdom.Element;
import org.jdom.Namespace;

import java.io.StringReader;
import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

public class SubWorkflowActionExecutor extends ActionExecutor {
    public static final String ACTION_TYPE = "sub-workflow";
    public static final String LOCAL = "local";

    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<String>();

    static {
        String[] badUserProps = {PropertiesUtils.DAYS, PropertiesUtils.HOURS, PropertiesUtils.MINUTES,
                PropertiesUtils.KB, PropertiesUtils.MB, PropertiesUtils.GB, PropertiesUtils.TB, PropertiesUtils.PB,
                PropertiesUtils.RECORDS, PropertiesUtils.MAP_IN, PropertiesUtils.MAP_OUT, PropertiesUtils.REDUCE_IN,
                PropertiesUtils.REDUCE_OUT, PropertiesUtils.GROUPS};

        String[] badDefaultProps = {PropertiesUtils.HADOOP_USER};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_DEFAULT_PROPERTIES);
        PropertiesUtils.createPropertySet(badDefaultProps, DISALLOWED_DEFAULT_PROPERTIES);
    }

    protected SubWorkflowActionExecutor() {
        super(ACTION_TYPE);
    }

    public void initActionType() {
        super.initActionType();
    }

    protected OozieClient getWorkflowClient(Context context, String oozieUri) {
        OozieClient oozieClient;
        if (oozieUri.equals(LOCAL)) {
            WorkflowJobBean workflow = (WorkflowJobBean) context.getWorkflow();
            String user = workflow.getUser();
            String group = workflow.getGroup();
            String authToken = workflow.getAuthToken();
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user, authToken);
            oozieClient = new LocalOozieClient(dagEngine);
        }
        else {
            // TODO we need to add authToken to the WC for the remote case
            oozieClient = new OozieClient(oozieUri);
        }
        return oozieClient;
    }

    protected void injectInline(Element eConf, Configuration subWorkflowConf) throws IOException,
            ActionExecutorException {
        if (eConf != null) {
            String strConf = XmlUtils.prettyPrint(eConf).toString();
            Configuration conf = new XConfiguration(new StringReader(strConf));
            try {
                PropertiesUtils.checkDisallowedProperties(conf, DISALLOWED_DEFAULT_PROPERTIES);
            }
            catch (CommandException ex) {
                throw convertException(ex);
            }
            XConfiguration.copy(conf, subWorkflowConf);
        }
    }

    @SuppressWarnings("unchecked")
    protected void injectCallback(Context context, Configuration conf) {
        String callback = context.getCallbackUrl("$status");
        if (conf.get(OozieClient.WORKFLOW_NOTIFICATION_URL) != null) {
            XLog.getLog(getClass())
                    .warn("Sub-Workflow configuration has a custom job end notification URI, overriding");
        }
        conf.set(OozieClient.WORKFLOW_NOTIFICATION_URL, callback);
    }

    protected void injectRecovery(String externalId, Configuration conf) {
        conf.set(OozieClient.EXTERNAL_ID, externalId);
    }

    protected String checkIfRunning(OozieClient oozieClient, String extId) throws OozieClientException {
        String jobId = oozieClient.getJobId(extId);
        if (jobId.equals("")) {
            return null;
        }
        return jobId;
    }

    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            Element eConf = XmlUtils.parseXml(action.getConf());
            Namespace ns = eConf.getNamespace();
            Element e = eConf.getChild("oozie", ns);
            String oozieUri = (e == null) ? LOCAL : e.getTextTrim();
            OozieClient oozieClient = getWorkflowClient(context, oozieUri);
            String subWorkflowId = null;
            String extId = context.getRecoveryId();
            String runningJobId = null;
            if (extId != null) {
                runningJobId = checkIfRunning(oozieClient, extId);
            }
            if (runningJobId == null) {
                String appPath = eConf.getChild("app-path", ns).getTextTrim();

                XConfiguration subWorkflowConf = new XConfiguration();
                Configuration parentConf = new XConfiguration(new StringReader(context.getWorkflow().getConf()));
                if (eConf.getChild(("propagate-configuration"), ns) != null) {
                    XConfiguration.copy(parentConf, subWorkflowConf);
                }

                // the proto has the necessary credentials
                Configuration protoActionConf = context.getProtoActionConf();
                XConfiguration.copy(protoActionConf, subWorkflowConf);
                subWorkflowConf.set(OozieClient.APP_PATH, appPath);
                String group = ConfigUtils.getWithDeprecatedCheck(parentConf, OozieClient.JOB_ACL, OozieClient.GROUP_NAME, null);
                if(group != null) {
                    subWorkflowConf.set(OozieClient.GROUP_NAME, group);
                }
                injectInline(eConf.getChild("configuration", ns), subWorkflowConf);
                injectCallback(context, subWorkflowConf);
                injectRecovery(extId, subWorkflowConf);

                //TODO: this has to be refactored later to be done in a single place for REST calls and this
                JobUtils.normalizeAppPath(context.getWorkflow().getUser(), context.getWorkflow().getGroup(),
                                          subWorkflowConf);

                subWorkflowId = oozieClient.run(subWorkflowConf.toProperties());
            }
            else {
                subWorkflowId = runningJobId;
            }
            WorkflowJob workflow = oozieClient.getJobInfo(subWorkflowId);
            String consoleUrl = workflow.getConsoleUrl();
            context.setStartData(subWorkflowId, oozieUri, consoleUrl);
            if (runningJobId != null) {
                check(context, action);
            }
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            String externalStatus = action.getExternalStatus();
            WorkflowAction.Status status = externalStatus.equals("SUCCEEDED") ? WorkflowAction.Status.OK
                                           : WorkflowAction.Status.ERROR;
            context.setEndData(status, getActionSignal(status));
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            String subWorkflowId = action.getExternalId();
            String oozieUri = action.getTrackerUri();
            OozieClient oozieClient = getWorkflowClient(context, oozieUri);
            WorkflowJob subWorkflow = oozieClient.getJobInfo(subWorkflowId);
            WorkflowJob.Status status = subWorkflow.getStatus();
            switch (status) {
                case FAILED:
                case KILLED:
                case SUCCEEDED:
                    context.setExecutionData(status.toString(), null);
                    break;
                default:
                    context.setExternalStatus(status.toString());
                    break;
            }
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            String subWorkflowId = action.getExternalId();
            String oozieUri = action.getTrackerUri();
            OozieClient oozieClient = getWorkflowClient(context, oozieUri);
            oozieClient.kill(subWorkflowId);
            context.setEndData(WorkflowAction.Status.KILLED, getActionSignal(WorkflowAction.Status.KILLED));
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    private static Set<String> FINAL_STATUS = new HashSet<String>();

    static {
        FINAL_STATUS.add("SUCCEEDED");
        FINAL_STATUS.add("KILLED");
        FINAL_STATUS.add("FAILED");
    }

    public boolean isCompleted(String externalStatus) {
        return FINAL_STATUS.contains(externalStatus);
    }
}
