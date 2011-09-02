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
package org.apache.oozie.command.wf;

import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.command.Command;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.lite.NodeHandler;

public class ReRunCommand extends WorkflowCommand<Void> {

    private String jobId;
    private Configuration conf;
    private String authToken;
    private Set<String> nodesToSkip = new HashSet<String>();
    public static final String TO_SKIP = "TO_SKIP";

    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<String>();
    private static final Set<String> DISALLOWED_USER_PROPERTIES = new HashSet<String>();

    static {
        String[] badUserProps = {PropertiesUtils.DAYS, PropertiesUtils.HOURS, PropertiesUtils.MINUTES,
                PropertiesUtils.KB, PropertiesUtils.MB, PropertiesUtils.GB, PropertiesUtils.TB, PropertiesUtils.PB,
                PropertiesUtils.RECORDS, PropertiesUtils.MAP_IN, PropertiesUtils.MAP_OUT, PropertiesUtils.REDUCE_IN,
                PropertiesUtils.REDUCE_OUT, PropertiesUtils.GROUPS};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_USER_PROPERTIES);

        String[] badDefaultProps = {PropertiesUtils.HADOOP_USER, PropertiesUtils.HADOOP_UGI,
                WorkflowAppService.HADOOP_JT_KERBEROS_NAME, WorkflowAppService.HADOOP_NN_KERBEROS_NAME};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_DEFAULT_PROPERTIES);
        PropertiesUtils.createPropertySet(badDefaultProps, DISALLOWED_DEFAULT_PROPERTIES);
    }

    public ReRunCommand(String jobId, Configuration conf, String authToken) {
        super("rerun", "rerun", 1, XLog.STD);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.conf = ParamChecker.notNull(conf, "conf");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

    /**
     * Checks the pre-conditions that are required for workflow to recover - Last run of Workflow should be completed -
     * The nodes that are to be skipped are to be completed successfully in the base run.
     *
     * @param wfBean Workflow bean
     * @param actions List of actions of Workflow
     * @throws org.apache.oozie.command.CommandException On failure of pre-conditions
     */
    private void checkPreConditions(WorkflowJobBean wfBean, List<WorkflowActionBean> actions) throws CommandException {
        if (!(wfBean.getStatus().equals(WorkflowJob.Status.FAILED)
                || wfBean.getStatus().equals(WorkflowJob.Status.KILLED) || wfBean.getStatus().equals(
                WorkflowJob.Status.SUCCEEDED))) {
            throw new CommandException(ErrorCode.E0805, wfBean.getStatus());
        }
        Set<String> unmachedNodes = new HashSet<String>(nodesToSkip);
        for (WorkflowActionBean action : actions) {
            if (nodesToSkip.contains(action.getName())) {
                if (!action.getStatus().equals(WorkflowAction.Status.OK)
                        && !action.getStatus().equals(WorkflowAction.Status.ERROR)) {
                    throw new CommandException(ErrorCode.E0806, action.getName());
                }
                unmachedNodes.remove(action.getName());
            }
        }
        if (unmachedNodes.size() > 0) {
            StringBuilder sb = new StringBuilder();
            String separator = "";
            for (String s : unmachedNodes) {
                sb.append(separator).append(s);
                separator = ",";
            }
            throw new CommandException(ErrorCode.E0807, sb);
        }
    }

    /**
     * Parses the config and adds the nodes that are to be skipped to the skipped node list
     */
    private void parseSkippedNodeConf() {
        if (conf != null) {
            Collection<String> skipNodes = conf.getStringCollection(OozieClient.RERUN_SKIP_NODES);
            for (String str : skipNodes) {
                // trimming is required
                nodesToSkip.add(str.trim());
            }
        }
    }

    protected Void call(WorkflowStore store) throws StoreException, CommandException {
        incrJobCounter(1);
        WorkflowJobBean wfBean = store.getWorkflow(jobId, false);
        setLogInfo(wfBean);
        List<WorkflowActionBean> actions = store.getActionsForWorkflow(jobId, false);
        WorkflowInstance oldWfInstance = wfBean.getWorkflowInstance();
        WorkflowInstance newWfInstance;
        XLog log = XLog.getLog(getClass());
        parseSkippedNodeConf();
        checkPreConditions(wfBean, actions);

        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        try {
            XLog.Info.get().setParameter(DagXLogInfoService.TOKEN, conf.get(OozieClient.LOG_TOKEN));
            WorkflowApp app = wps.parseDef(conf, authToken);
            XConfiguration protoActionConf = wps.createProtoActionConf(conf, authToken, true);
            WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();

            Path configDefault = new Path(new Path(conf.get(OozieClient.APP_PATH)).getParent(), SubmitCommand.CONFIG_DEFAULT);
            FileSystem fs = Services.get().get(HadoopAccessorService.class).createFileSystem(wfBean.getUser(),
                                                                                             wfBean.getGroup(), configDefault.toUri(), new Configuration());

            if (fs.exists(configDefault)) {
                Configuration defaultConf = new XConfiguration(fs.open(configDefault));
                PropertiesUtils.checkDisallowedProperties(defaultConf, DISALLOWED_DEFAULT_PROPERTIES);
                XConfiguration.injectDefaults(defaultConf, conf);
            }

            PropertiesUtils.checkDisallowedProperties(conf, DISALLOWED_USER_PROPERTIES);

            try {
                newWfInstance = workflowLib.createInstance(app, conf, jobId);
            }
            catch (WorkflowException e) {
                throw new StoreException(e);
            }
            wfBean.setAppName(app.getName());
            wfBean.setProtoActionConf(protoActionConf.toXmlString());
        }
        catch (WorkflowException ex) {
            throw new CommandException(ex);
        }
        catch (IOException ex) {
            throw new CommandException(ErrorCode.E0803, ex);
        }
        catch (HadoopAccessorException e) {
            throw new CommandException(e);
        }

        for (int i = 0; i < actions.size(); i++) {
            if (!nodesToSkip.contains(actions.get(i).getName())) {
                store.deleteAction(actions.get(i).getId());
                log.info("Deleting Action[{0}] for re-run", actions.get(i).getId());
            }
            else {
                copyActionData(newWfInstance, oldWfInstance);
            }
        }

        wfBean.setAppPath(conf.get(OozieClient.APP_PATH));
        wfBean.setConf(XmlUtils.prettyPrint(conf).toString());
        wfBean.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
        wfBean.setUser(conf.get(OozieClient.USER_NAME));
        wfBean.setGroup(conf.get(OozieClient.GROUP_NAME));
        wfBean.setExternalId(conf.get(OozieClient.EXTERNAL_ID));
        wfBean.setEndTime(null);
        wfBean.setRun(wfBean.getRun() + 1);
        wfBean.setStatus(WorkflowJob.Status.PREP);
        wfBean.setWorkflowInstance(newWfInstance);
        store.updateWorkflow(wfBean);
        return null;
    }

    /**
     * Copys the variables for skipped nodes from the old wfInstance to new one.
     *
     * @param newWfInstance
     * @param oldWfInstance
     */
    private void copyActionData(WorkflowInstance newWfInstance, WorkflowInstance oldWfInstance) {
        Map<String, String> oldVars = new HashMap<String, String>();
        Map<String, String> newVars = new HashMap<String, String>();
        oldVars = oldWfInstance.getAllVars();
        for (String var : oldVars.keySet()) {
            String actionName = var.split(WorkflowInstance.NODE_VAR_SEPARATOR)[0];
            if (nodesToSkip.contains(actionName)) {
                newVars.put(var, oldVars.get(var));
            }
        }
        for (String node : nodesToSkip) {
            // Setting the TO_SKIP variable to true. This will be used by
            // SignalCommand and LiteNodeHandler to skip the action.
            newVars.put(node + WorkflowInstance.NODE_VAR_SEPARATOR + TO_SKIP, "true");
            String visitedFlag = NodeHandler.getLoopFlag(node);
            // Removing the visited flag so that the action won't be considered
            // a loop.
            if (newVars.containsKey(visitedFlag)) {
                newVars.remove(visitedFlag);
            }
        }
        newWfInstance.setAllVars(newVars);
    }

    @Override
    protected Void execute(WorkflowStore store) throws CommandException, StoreException {
        try {
            XLog.getLog(getClass()).debug("STARTED ReRunCommand for job " + jobId);
            if (lock(jobId)) {
                call(store);
            }
            else {
                queueCallable(new ReRunCommand(jobId, conf, authToken), LOCK_FAILURE_REQUEUE_INTERVAL);
                XLog.getLog(getClass()).warn("ReRunCommand lock was not acquired - failed {0}", jobId);
            }
        }
        catch (InterruptedException e) {
            queueCallable(new ReRunCommand(jobId, conf, authToken), LOCK_FAILURE_REQUEUE_INTERVAL);
            XLog.getLog(getClass())
                    .warn("ReRunCommand lock was not acquired - interrupted exception failed {0}", jobId);
        }
        XLog.getLog(getClass()).debug("ENDED ReRunCommand for job " + jobId);
        return null;
    }
}
