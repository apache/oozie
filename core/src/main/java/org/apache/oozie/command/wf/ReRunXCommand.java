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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.AppType;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.oozie.SubWorkflowActionExecutor;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionQueryExecutor.WorkflowActionQuery;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.sla.SLAOperations;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.ELUtils;
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
import org.jdom.Element;
import org.jdom.JDOMException;

/**
 * This is a RerunXCommand which is used for rerunn.
 *
 */
public class ReRunXCommand extends WorkflowXCommand<Void> {
    private final String jobId;
    private Configuration conf;
    private final Set<String> nodesToSkip = new HashSet<String>();
    public static final String TO_SKIP = "TO_SKIP";
    private WorkflowJobBean wfBean;
    private List<WorkflowActionBean> actions;
    private List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
    private List<JsonBean> deleteList = new ArrayList<JsonBean>();

    private static final Set<String> DISALLOWED_DEFAULT_PROPERTIES = new HashSet<String>();
    private static final Set<String> DISALLOWED_USER_PROPERTIES = new HashSet<String>();
    public static final String DISABLE_CHILD_RERUN = "oozie.wf.rerun.disablechild";

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

    public ReRunXCommand(String jobId, Configuration conf) {
        super("rerun", "rerun", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.conf = ParamChecker.notNull(conf, "conf");
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(jobId);
    }

    @Override
    protected Void execute() throws CommandException {
        setupReRun();
        startWorkflow(jobId);
        return null;
    }

    private void startWorkflow(String jobId) throws CommandException {
        new StartXCommand(jobId).call();
    }

    private void setupReRun() throws CommandException {
        InstrumentUtils.incrJobCounter(getName(), 1, getInstrumentation());
        LogUtils.setLogInfo(wfBean);
        WorkflowInstance oldWfInstance = this.wfBean.getWorkflowInstance();
        WorkflowInstance newWfInstance;
        String appPath = null;

        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        try {
            XLog.Info.get().setParameter(DagXLogInfoService.TOKEN, conf.get(OozieClient.LOG_TOKEN));
            WorkflowApp app = wps.parseDef(conf, null);
            XConfiguration protoActionConf = wps.createProtoActionConf(conf, true);
            WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();

            appPath = conf.get(OozieClient.APP_PATH);
            URI uri = new URI(appPath);
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            Configuration fsConf = has.createConfiguration(uri.getAuthority());
            FileSystem fs = has.createFileSystem(wfBean.getUser(), uri, fsConf);

            Path configDefault = null;
            // app path could be a directory
            Path path = new Path(uri.getPath());
            if (!fs.isFile(path)) {
                configDefault = new Path(path, SubmitXCommand.CONFIG_DEFAULT);
            }
            else {
                configDefault = new Path(path.getParent(), SubmitXCommand.CONFIG_DEFAULT);
            }

            if (fs.exists(configDefault)) {
                Configuration defaultConf = new XConfiguration(fs.open(configDefault));
                PropertiesUtils.checkDisallowedProperties(defaultConf, DISALLOWED_DEFAULT_PROPERTIES);
                XConfiguration.injectDefaults(defaultConf, conf);
            }

            PropertiesUtils.checkDisallowedProperties(conf, DISALLOWED_USER_PROPERTIES);

            // Resolving all variables in the job properties. This ensures the Hadoop Configuration semantics are
            // preserved. The Configuration.get function within XConfiguration.resolve() works recursively to get the
            // final value corresponding to a key in the map Resetting the conf to contain all the resolved values is
            // necessary to ensure propagation of Oozie properties to Hadoop calls downstream
            conf = ((XConfiguration) conf).resolve();

            try {
                newWfInstance = workflowLib.createInstance(app, conf, jobId);
            }
            catch (WorkflowException e) {
                throw new CommandException(e);
            }
            String appName = ELUtils.resolveAppName(app.getName(), conf);
            if (SLAService.isEnabled()) {
                Element wfElem = XmlUtils.parseXml(app.getDefinition());
                ELEvaluator evalSla = SubmitXCommand.createELEvaluatorForGroup(conf, "wf-sla-submit");
                Element eSla = XmlUtils.getSLAElement(wfElem);
                String jobSlaXml = null;
                if (eSla != null) {
                    jobSlaXml = SubmitXCommand.resolveSla(eSla, evalSla);
                }
                writeSLARegistration(wfElem, jobSlaXml, newWfInstance.getId(),
                        conf.get(SubWorkflowActionExecutor.PARENT_ID), conf.get(OozieClient.USER_NAME), appName,
                        evalSla);
            }
            wfBean.setAppName(appName);
            wfBean.setProtoActionConf(protoActionConf.toXmlString());
        }
        catch (WorkflowException ex) {
            throw new CommandException(ex);
        }
        catch (IOException ex) {
            throw new CommandException(ErrorCode.E0803, ex.getMessage(), ex);
        }
        catch (HadoopAccessorException ex) {
            throw new CommandException(ex);
        }
        catch (URISyntaxException ex) {
            throw new CommandException(ErrorCode.E0711, appPath, ex.getMessage(), ex);
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E1007, ex.getMessage(), ex);
        }

        for (int i = 0; i < actions.size(); i++) {
            // Skipping to delete the sub workflow when rerun failed node option has been provided. As same
            // action will be used to rerun the job.
            if (!nodesToSkip.contains(actions.get(i).getName()) &&
                    !(conf.getBoolean(OozieClient.RERUN_FAIL_NODES, false) &&
                    SubWorkflowActionExecutor.ACTION_TYPE.equals(actions.get(i).getType()))) {
                deleteList.add(actions.get(i));
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

        try {
            wfBean.setLastModifiedTime(new Date());
            updateList.add(new UpdateEntry<WorkflowJobQuery>(WorkflowJobQuery.UPDATE_WORKFLOW_RERUN, wfBean));
            // call JPAExecutor to do the bulk writes
            BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(null, updateList, deleteList);
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        finally {
            updateParentIfNecessary(wfBean);
        }

    }

    @SuppressWarnings("unchecked")
	private void writeSLARegistration(Element wfElem, String jobSlaXml, String id, String parentId, String user,
            String appName, ELEvaluator evalSla) throws JDOMException, CommandException {
        if (jobSlaXml != null && jobSlaXml.length() > 0) {
            Element eSla = XmlUtils.parseXml(jobSlaXml);
            // insert into new table
            SLAOperations.createSlaRegistrationEvent(eSla, jobId, parentId, AppType.WORKFLOW_JOB, user, appName, LOG,
                    true);
        }
        // Add sla for wf actions
        for (Element action : (List<Element>) wfElem.getChildren("action", wfElem.getNamespace())) {
            Element actionSla = XmlUtils.getSLAElement(action);
            if (actionSla != null) {
                String actionSlaXml = SubmitXCommand.resolveSla(actionSla, evalSla);
                actionSla = XmlUtils.parseXml(actionSlaXml);
                if (!nodesToSkip.contains(action.getAttributeValue("name"))) {
                    String actionId = Services.get().get(UUIDService.class)
                            .generateChildId(jobId, action.getAttributeValue("name") + "");
                    SLAOperations.createSlaRegistrationEvent(actionSla, actionId, jobId, AppType.WORKFLOW_ACTION, user,
                            appName, LOG, true);
                }
            }
        }

    }

    /**
     * Loading the Wfjob and workflow actions. Parses the config and adds the nodes that are to be skipped to the
     * skipped node list
     *
     * @throws CommandException
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        try {
            this.wfBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_STATUS, this.jobId);
            this.actions = WorkflowActionQueryExecutor.getInstance().getList(
                    WorkflowActionQuery.GET_ACTIONS_FOR_WORKFLOW_RERUN, this.jobId);

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
            throw new CommandException(ErrorCode.E0603, ex.getMessage(), ex);
        }
    }

    /**
     * Checks the pre-conditions that are required for workflow to recover - Last run of Workflow should be completed -
     * The nodes that are to be skipped are to be completed successfully in the base run.
     *
     * @throws CommandException
     * @throws PreconditionException On failure of pre-conditions
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        // Throwing error if parent exist and same workflow trying to rerun, when running child workflow disabled
        // through conf.
        if (wfBean.getParentId() != null && !conf.getBoolean(SubWorkflowActionExecutor.SUBWORKFLOW_RERUN, false)
                && ConfigurationService.getBoolean(DISABLE_CHILD_RERUN)) {
            throw new CommandException(ErrorCode.E0755, " Rerun is not allowed through child workflow, please" +
                    " re-run through the parent " + wfBean.getParentId());
        }

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

    @Override
    public String getEntityKey() {
        return this.jobId;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            this.wfBean = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_RERUN, this.jobId);
            this.actions = WorkflowActionQueryExecutor.getInstance().getList(
                    WorkflowActionQuery.GET_ACTIONS_FOR_WORKFLOW_RERUN, this.jobId);
        }
        catch (JPAExecutorException jpe) {
            throw new CommandException(jpe);
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        eagerVerifyPrecondition();
    }
}
