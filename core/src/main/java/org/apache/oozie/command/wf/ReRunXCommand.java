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
package org.apache.oozie.command.wf;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionDeleteJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionsGetForJobJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobUpdateJPAExecutor;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.workflow.lite.NodeHandler;

/**
 * This is a RerunXCommand which is used for rerunn.
 *
 */
public class ReRunXCommand extends WorkflowXCommand<Void> {
    private final String jobId;
    private Configuration conf;
    private final String authToken;
    private final Set<String> nodesToSkip = new HashSet<String>();
    public static final String TO_SKIP = "TO_SKIP";
    private WorkflowJobBean wfBean;
    private List<WorkflowActionBean> actions;
    private JPAService jpaService;

    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<String>();
    private static final Set<String> DISALLOWED_USER_PROPERTIES = new HashSet<String>();

    static {
        String[] badUserProps = { PropertiesUtils.DAYS, PropertiesUtils.HOURS, PropertiesUtils.MINUTES,
                PropertiesUtils.KB, PropertiesUtils.MB, PropertiesUtils.GB, PropertiesUtils.TB, PropertiesUtils.PB,
                PropertiesUtils.RECORDS, PropertiesUtils.MAP_IN, PropertiesUtils.MAP_OUT, PropertiesUtils.REDUCE_IN,
                PropertiesUtils.REDUCE_OUT, PropertiesUtils.GROUPS };
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_USER_PROPERTIES);

        String[] badDefaultProps = { PropertiesUtils.HADOOP_USER};
        PropertiesUtils.createPropertySet(badUserProps, DISALLOWED_DEFAULT_PROPERTIES);
        PropertiesUtils.createPropertySet(badDefaultProps, DISALLOWED_DEFAULT_PROPERTIES);
    }

    public ReRunXCommand(String jobId, Configuration conf, String authToken) {
        super("rerun", "rerun", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.conf = ParamChecker.notNull(conf, "conf");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
        LogUtils.setLogInfo(wfBean, logInfo);
        WorkflowInstance oldWfInstance = this.wfBean.getWorkflowInstance();
        WorkflowInstance newWfInstance;

        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        try {
            XLog.Info.get().setParameter(DagXLogInfoService.TOKEN, conf.get(OozieClient.LOG_TOKEN));
            WorkflowApp app = wps.parseDef(conf, authToken);
            XConfiguration protoActionConf = wps.createProtoActionConf(conf, authToken, true);
            WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();

            URI uri = new URI(conf.get(OozieClient.APP_PATH));
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createJobConf(uri.getAuthority());
            FileSystem fs = has.createFileSystem(wfBean.getUser(), uri, fsConf);

            Path configDefault = null;
            // app path could be a directory
            Path path = new Path(uri.getPath());
            if (!fs.isFile(path)) {
                configDefault = new Path(path, SubmitXCommand.CONFIG_DEFAULT);
            } else {
                configDefault = new Path(path.getParent(), SubmitXCommand.CONFIG_DEFAULT);
            }

            if (fs.exists(configDefault)) {
                Configuration defaultConf = new XConfiguration(fs.open(configDefault));
                PropertiesUtils.checkDisallowedProperties(defaultConf, DISALLOWED_DEFAULT_PROPERTIES);
                XConfiguration.injectDefaults(defaultConf, conf);
            }

            PropertiesUtils.checkDisallowedProperties(conf, DISALLOWED_USER_PROPERTIES);

            // Resolving all variables in the job properties. This ensures the Hadoop Configuration semantics are preserved.
            // The Configuration.get function within XConfiguration.resolve() works recursively to get the final value corresponding to a key in the map
            // Resetting the conf to contain all the resolved values is necessary to ensure propagation of Oozie properties to Hadoop calls downstream
            conf = ((XConfiguration) conf).resolve();

            try {
                newWfInstance = workflowLib.createInstance(app, conf, jobId);
            }
            catch (WorkflowException e) {
                throw new CommandException(e);
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
        catch (HadoopAccessorException ex) {
            throw new CommandException(ex);
        }
        catch (URISyntaxException ex) {
            throw new CommandException(ErrorCode.E0711, ex.getMessage(), ex);
        }

        try {
            for (int i = 0; i < actions.size(); i++) {
                if (!nodesToSkip.contains(actions.get(i).getName())) {
                    jpaService.execute(new WorkflowActionDeleteJPAExecutor(actions.get(i).getId()));
                    LOG.info("Deleting Action[{0}] for re-run", actions.get(i).getId());
                }
                else {
                    copyActionData(newWfInstance, oldWfInstance);
                }
            }

            wfBean.setAppPath(conf.get(OozieClient.APP_PATH));
            wfBean.setConf(XmlUtils.prettyPrint(conf).toString());
            wfBean.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
            wfBean.setUser(conf.get(OozieClient.USER_NAME));
            String group = ConfigUtils.getWithDeprecatedCheck(conf, OozieClient.JOB_ACL, OozieClient.GROUP_NAME, null);
            wfBean.setGroup(group);
            wfBean.setExternalId(conf.get(OozieClient.EXTERNAL_ID));
            wfBean.setEndTime(null);
            wfBean.setRun(wfBean.getRun() + 1);
            wfBean.setStatus(WorkflowJob.Status.PREP);
            wfBean.setWorkflowInstance(newWfInstance);
            jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfBean));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }

        return null;
    }

    /**
     * Loading the Wfjob and workflow actions. Parses the config and adds the nodes that are to be skipped to the
     * skipped node list
     *
     * @throws CommandException
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        super.eagerLoadState();
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.wfBean = jpaService.execute(new WorkflowJobGetJPAExecutor(this.jobId));
                this.actions = jpaService.execute(new WorkflowActionsGetForJobJPAExecutor(this.jobId));
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }

            if (conf != null) {
                if (conf.getBoolean(OozieClient.RERUN_FAIL_NODES, false) == false) { //Rerun with skipNodes
                    Collection<String> skipNodes = conf.getStringCollection(OozieClient.RERUN_SKIP_NODES);
                    for (String str : skipNodes) {
                        // trimming is required
                        nodesToSkip.add(str.trim());
                    }
                    LOG.debug("Skipnode size :" + nodesToSkip.size());
                }
                else {
                    for (WorkflowActionBean action : actions) { // Rerun from failed nodes
                        if (action.getStatus() == WorkflowAction.Status.OK) {
                            nodesToSkip.add(action.getName());
                        }
                    }
                    LOG.debug("Skipnode size are to rerun from FAIL nodes :" + nodesToSkip.size());
                }
                StringBuilder tmp = new StringBuilder();
                for (String node : nodesToSkip) {
                    tmp.append(node).append(",");
                }
                LOG.debug("SkipNode List :" + tmp);
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex);
        }
    }

    /**
     * Checks the pre-conditions that are required for workflow to recover - Last run of Workflow should be completed -
     * The nodes that are to be skipped are to be completed successfully in the base run.
     *
     * @throws org.apache.oozie.command.CommandException,PreconditionException On failure of pre-conditions
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        super.eagerVerifyPrecondition();
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
     * Copys the variables for skipped nodes from the old wfInstance to new one.
     *
     * @param newWfInstance : Source WF instance object
     * @param oldWfInstance : Update WF instance
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

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return this.jobId;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }
}
