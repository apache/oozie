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
import org.apache.oozie.executor.jpa.BulkUpdateInsertForCoordActionStartJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.jdom.Element;
import org.jdom.JDOMException;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CoordActionStartXCommand extends CoordinatorXCommand<Void> {

    public static final String EL_ERROR = "EL_ERROR";
    public static final String EL_EVAL_ERROR = "EL_EVAL_ERROR";
    public static final String COULD_NOT_START = "COULD_NOT_START";
    public static final String START_DATA_MISSING = "START_DATA_MISSING";
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";

    private final XLog log = getLog();
    private String actionId = null;
    private String user = null;
    private String authToken = null;
    private CoordinatorActionBean coordAction = null;
    private JPAService jpaService = null;
    private String jobId = null;
    private List<JsonBean> updateList = new ArrayList<JsonBean>();
    private List<JsonBean> insertList = new ArrayList<JsonBean>();

    public CoordActionStartXCommand(String id, String user, String token, String jobId) {
        //super("coord_action_start", "coord_action_start", 1, XLog.OPS);
        super("coord_action_start", "coord_action_start", 1);
        this.actionId = ParamChecker.notEmpty(id, "id");
        this.user = ParamChecker.notEmpty(user, "user");
        this.authToken = ParamChecker.notEmpty(token, "token");
        this.jobId = jobId;
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
        Element configElement = (Element) workflowProperties.getChild("action", workflowProperties.getNamespace())
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
        String appPath = workflowProperties.getChild("action", workflowProperties.getNamespace()).getChild("workflow",
                                                                                                           workflowProperties.getNamespace()).getChild("app-path", workflowProperties.getNamespace()).getValue();
        runConf.set("oozie.wf.application.path", appPath);
        return runConf;
    }

    @Override
    protected Void execute() throws CommandException {
        boolean makeFail = true;
        String errCode = "";
        String errMsg = "";
        ParamChecker.notEmpty(user, "user");
        ParamChecker.notEmpty(authToken, "authToken");

        log.debug("actionid=" + actionId + ", status=" + coordAction.getStatus());
        if (coordAction.getStatus() == CoordinatorAction.Status.SUBMITTED) {
            // log.debug("getting.. job id: " + coordAction.getJobId());
            // create merged runConf to pass to WF Engine
            Configuration runConf = mergeConfig(coordAction);
            coordAction.setRunConf(XmlUtils.prettyPrint(runConf).toString());
            // log.debug("%%% merged runconf=" +
            // XmlUtils.prettyPrint(runConf).toString());
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user, authToken);
            try {
                boolean startJob = true;
                Configuration conf = new XConfiguration(new StringReader(coordAction.getRunConf()));
                SLAEventBean slaEvent = SLADbOperations.createStatusEvent(coordAction.getSlaXml(), coordAction.getId(), Status.STARTED,
                        SlaAppType.COORDINATOR_ACTION, log);
                if(slaEvent != null) {
                    insertList.add(slaEvent);
                }

                // Normalize workflow appPath here;
                JobUtils.normalizeAppPath(conf.get(OozieClient.USER_NAME), conf.get(OozieClient.GROUP_NAME), conf);
                String wfId = dagEngine.submitJob(conf, startJob);
                coordAction.setStatus(CoordinatorAction.Status.RUNNING);
                coordAction.setExternalId(wfId);
                coordAction.incrementAndGetPending();

                //store.updateCoordinatorAction(coordAction);
                JPAService jpaService = Services.get().get(JPAService.class);
                if (jpaService != null) {
                    log.debug("Updating WF record for WFID :" + wfId + " with parent id: " + actionId);
                    WorkflowJobBean wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(wfId));
                    wfJob.setParentId(actionId);
                    wfJob.setLastModifiedTime(new Date());
                    updateList.add(wfJob);
                    updateList.add(coordAction);
                    try {
                        jpaService.execute(new BulkUpdateInsertForCoordActionStartJPAExecutor(updateList, insertList));
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

                    updateList = new ArrayList<JsonBean>();
                    updateList.add(coordAction);
                    insertList = new ArrayList<JsonBean>();

                    SLAEventBean slaEvent = SLADbOperations.createStatusEvent(coordAction.getSlaXml(), coordAction.getId(), Status.FAILED,
                            SlaAppType.COORDINATOR_ACTION, log);
                    if(slaEvent != null) {
                        insertList.add(slaEvent); //Update SLA events
                    }
                    try {
                        // call JPAExecutor to do the bulk writes
                        jpaService.execute(new BulkUpdateInsertForCoordActionStartJPAExecutor(updateList, insertList));
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

    }

    @Override
    protected void eagerLoadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }

        try {
            coordAction = jpaService.execute(new org.apache.oozie.executor.jpa.CoordActionGetForStartJPAExecutor(actionId));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        LogUtils.setLogInfo(coordAction, logInfo);
    }

    @Override
    protected void verifyPrecondition() throws PreconditionException {
        if (coordAction.getStatus() != CoordinatorAction.Status.SUBMITTED) {
            throw new PreconditionException(ErrorCode.E1100, "The coord action [" + actionId + "] must have status "
                    + CoordinatorAction.Status.SUBMITTED.name() + " but has status [" + coordAction.getStatus().name() + "]");
        }
    }
}
