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

package org.apache.oozie.command.coord;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.hadoop.OozieJobInfo;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.DagEngine;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.db.SLADbOperations;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.executor.jpa.BatchQueryExecutor.UpdateEntry;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.CoordActionQueryExecutor.CoordActionQuery;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobQueryExecutor.WorkflowJobQuery;
import org.jdom.Element;
import org.jdom.JDOMException;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@SuppressWarnings("deprecation")
public class CoordActionStartXCommand extends CoordinatorXCommand<Void> {

    public static final String EL_ERROR = "EL_ERROR";
    public static final String EL_EVAL_ERROR = "EL_EVAL_ERROR";
    public static final String COULD_NOT_START = "COULD_NOT_START";
    public static final String START_DATA_MISSING = "START_DATA_MISSING";
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";

    private final XLog log = getLog();
    private String actionId = null;
    private String user = null;
    private String appName = null;
    private CoordinatorActionBean coordAction = null;
    private JPAService jpaService = null;
    private String jobId = null;
    private List<UpdateEntry> updateList = new ArrayList<UpdateEntry>();
    private List<JsonBean> insertList = new ArrayList<JsonBean>();

    public CoordActionStartXCommand(String id, String user, String appName, String jobId) {
        //super("coord_action_start", "coord_action_start", 1, XLog.OPS);
        super("coord_action_start", "coord_action_start", 1);
        this.actionId = ParamChecker.notEmpty(id, "id");
        this.user = ParamChecker.notEmpty(user, "user");
        this.appName = ParamChecker.notEmpty(appName, "appName");
        this.jobId = jobId;
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(actionId);
    }

    /**
     * Create config to pass to WF Engine 1. Get createdConf from coord_actions table 2. Get actionXml from
     * coord_actions table. Extract all 'property' tags and merge createdConf (overwrite duplicate keys). 3. Extract
     * 'app-path' from actionXML. Create a new property called 'oozie.wf.application.path' and merge with createdConf
     * (overwrite duplicate keys) 4. Read contents of config-default.xml in workflow directory. 5. Merge createdConf
     * with config-default.xml (overwrite duplicate keys). 6. Results is runConf which is saved in coord_actions table.
     * Merge Action createdConf with actionXml to create new runConf with replaced variables
     *
     * @param action CoordinatorActionBean
     * @return Configuration
     * @throws CommandException
     */
    private Configuration mergeConfig(CoordinatorActionBean action) throws CommandException {
        String createdConf = action.getCreatedConf();
        String actionXml = action.getActionXml();
        Element workflowProperties = null;
        try {
            workflowProperties = XmlUtils.parseXml(actionXml);
        }
        catch (JDOMException e1) {
            log.warn("Configuration parse error in:" + actionXml);
            throw new CommandException(ErrorCode.E1005, e1.getMessage(), e1);
        }
        // generate the 'runConf' for this action
        // Step 1: runConf = createdConf
        Configuration runConf = null;
        try {
            runConf = new XConfiguration(new StringReader(createdConf));

        }
        catch (IOException e1) {
            log.warn("Configuration parse error in:" + createdConf);
            throw new CommandException(ErrorCode.E1005, e1.getMessage(), e1);
        }
        // Step 2: Merge local properties into runConf
        // extract 'property' tags under 'configuration' block in the
        // coordinator.xml (saved in actionxml column)
        // convert Element to XConfiguration
        Element configElement = workflowProperties.getChild("action", workflowProperties.getNamespace())
                .getChild("workflow", workflowProperties.getNamespace()).getChild("configuration",
                                                                                  workflowProperties.getNamespace());
        if (configElement != null) {
            String strConfig = XmlUtils.prettyPrint(configElement).toString();
            Configuration localConf;
            try {
                localConf = new XConfiguration(new StringReader(strConfig));
            }
            catch (IOException e1) {
                log.warn("Configuration parse error in:" + strConfig);
                throw new CommandException(ErrorCode.E1005, e1.getMessage(), e1);
            }

            // copy configuration properties in coordinator.xml to the runConf
            XConfiguration.copy(localConf, runConf);
        }

        // Step 3: Extract value of 'app-path' in actionxml, and save it as a
        // new property called 'oozie.wf.application.path'
        // WF Engine requires the path to the workflow.xml to be saved under
        // this property name
        String appPath = workflowProperties.getChild("action", workflowProperties.getNamespace())
                .getChild("workflow", workflowProperties.getNamespace()).getChild("app-path",
                        workflowProperties.getNamespace()).getValue();

        // Copying application path in runconf.
        runConf.set("oozie.wf.application.path", appPath);

        // Step 4: Extract the runconf and copy the rerun config to runconf.
        if (runConf.get(CoordRerunXCommand.RERUN_CONF) != null) {
            Configuration rerunConf = null;
            try {
                rerunConf = new XConfiguration(new StringReader(runConf.get(CoordRerunXCommand.RERUN_CONF)));
                XConfiguration.copy(rerunConf, runConf);
            } catch (IOException e) {
                log.warn("Configuration parse error in:" + rerunConf);
                throw new CommandException(ErrorCode.E1005, e.getMessage(), e);
            }
            runConf.unset(CoordRerunXCommand.RERUN_CONF);
        }
        return runConf;
    }

    @Override
    protected Void execute() throws CommandException {
        boolean makeFail = true;
        String errCode = "";
        String errMsg = "";
        ParamChecker.notEmpty(user, "user");

        log.debug("actionid=" + actionId + ", status=" + coordAction.getStatus());
        if (coordAction.getStatus() == CoordinatorAction.Status.SUBMITTED) {
            // log.debug("getting.. job id: " + coordAction.getJobId());
            // create merged runConf to pass to WF Engine
            Configuration runConf = mergeConfig(coordAction);
            coordAction.setRunConf(XmlUtils.prettyPrint(runConf).toString());
            // log.debug("%%% merged runconf=" +
            // XmlUtils.prettyPrint(runConf).toString());
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user);
            try {
                Configuration conf = new XConfiguration(new StringReader(coordAction.getRunConf()));
                SLAEventBean slaEvent = SLADbOperations.createStatusEvent(coordAction.getSlaXml(), coordAction.getId(), Status.STARTED,
                        SlaAppType.COORDINATOR_ACTION, log);
                if(slaEvent != null) {
                    insertList.add(slaEvent);
                }
                if (OozieJobInfo.isJobInfoEnabled()) {
                    conf.set(OozieJobInfo.COORD_NAME, appName);
                    conf.set(OozieJobInfo.COORD_NOMINAL_TIME, coordAction.getNominalTimestamp().toString());
                }
                // Normalize workflow appPath here;
                JobUtils.normalizeAppPath(conf.get(OozieClient.USER_NAME), conf.get(OozieClient.GROUP_NAME), conf);
                if (coordAction.getExternalId() != null) {
                    conf.setBoolean(OozieClient.RERUN_FAIL_NODES, true);
                    dagEngine.reRun(coordAction.getExternalId(), conf);
                } else {
                    String wfId = dagEngine.submitJobFromCoordinator(conf, actionId);
                    coordAction.setExternalId(wfId);
                }
                coordAction.setStatus(CoordinatorAction.Status.RUNNING);
                coordAction.incrementAndGetPending();

                //store.updateCoordinatorAction(coordAction);
                JPAService jpaService = Services.get().get(JPAService.class);
                if (jpaService != null) {
                    log.debug("Updating WF record for WFID :" + coordAction.getExternalId() + " with parent id: " + actionId);
                    WorkflowJobBean wfJob = WorkflowJobQueryExecutor.getInstance().get(WorkflowJobQuery.GET_WORKFLOW_STARTTIME, coordAction.getExternalId());
                    wfJob.setParentId(actionId);
                    wfJob.setLastModifiedTime(new Date());
                    BatchQueryExecutor executor = BatchQueryExecutor.getInstance();
                    updateList.add(new UpdateEntry<WorkflowJobQuery>(
                            WorkflowJobQuery.UPDATE_WORKFLOW_PARENT_MODIFIED, wfJob));
                    updateList.add(new UpdateEntry<CoordActionQuery>(
                            CoordActionQuery.UPDATE_COORD_ACTION_FOR_START, coordAction));
                    try {
                        executor.executeBatchInsertUpdateDelete(insertList, updateList, null);
                        queue(new CoordActionNotificationXCommand(coordAction), 100);
                        if (EventHandlerService.isEnabled()) {
                            generateEvent(coordAction, user, appName, wfJob.getStartTime());
                        }
                    }
                    catch (JPAExecutorException je) {
                        throw new CommandException(je);
                    }
                }
                else {
                    log.error(ErrorCode.E0610);
                }

                makeFail = false;
            }
            catch (DagEngineException dee) {
                errMsg = dee.getMessage();
                errCode = dee.getErrorCode().toString();
                log.warn("can not create DagEngine for submitting jobs", dee);
            }
            catch (CommandException ce) {
                errMsg = ce.getMessage();
                errCode = ce.getErrorCode().toString();
                log.warn("command exception occured ", ce);
            }
            catch (java.io.IOException ioe) {
                errMsg = ioe.getMessage();
                errCode = "E1005";
                log.warn("Configuration parse error. read from DB :" + coordAction.getRunConf(), ioe);
            }
            catch (Exception ex) {
                errMsg = ex.getMessage();
                errCode = "E1005";
                log.warn("can not create DagEngine for submitting jobs", ex);
            }
            finally {
                if (makeFail == true) { // No DB exception occurs
                    log.error("Failing the action " + coordAction.getId() + ". Because " + errCode + " : " + errMsg);
                    coordAction.setStatus(CoordinatorAction.Status.FAILED);
                    if (errMsg.length() > 254) { // Because table column size is 255
                        errMsg = errMsg.substring(0, 255);
                    }
                    coordAction.setErrorMessage(errMsg);
                    coordAction.setErrorCode(errCode);

                    updateList = new ArrayList<UpdateEntry>();
                    updateList.add(new UpdateEntry<CoordActionQuery>(
                                    CoordActionQuery.UPDATE_COORD_ACTION_FOR_START, coordAction));
                    insertList = new ArrayList<JsonBean>();

                    SLAEventBean slaEvent = SLADbOperations.createStatusEvent(coordAction.getSlaXml(), coordAction.getId(), Status.FAILED,
                            SlaAppType.COORDINATOR_ACTION, log);
                    if(slaEvent != null) {
                        insertList.add(slaEvent); //Update SLA events
                    }
                    try {
                        // call JPAExecutor to do the bulk writes
                        BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(insertList, updateList, null);
                        if (EventHandlerService.isEnabled()) {
                            generateEvent(coordAction, user, appName, null);
                        }
                    }
                    catch (JPAExecutorException je) {
                        throw new CommandException(je);
                    }
                    queue(new CoordActionReadyXCommand(coordAction.getJobId()));
                }
            }
        }
        return null;
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
        jpaService = Services.get().get(JPAService.class);
        try {
            coordAction = jpaService.execute(new org.apache.oozie.executor.jpa.CoordActionGetForStartJPAExecutor(
                    actionId));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        LogUtils.setLogInfo(coordAction);
    }

    @Override
    protected void verifyPrecondition() throws PreconditionException {
        if (coordAction.getStatus() != CoordinatorAction.Status.SUBMITTED) {
            throw new PreconditionException(ErrorCode.E1100, "The coord action [" + actionId + "] must have status "
                    + CoordinatorAction.Status.SUBMITTED.name() + " but has status [" + coordAction.getStatus().name() + "]");
        }
    }

    @Override
    public String getKey(){
        return getName() + "_" + actionId;
    }
}
