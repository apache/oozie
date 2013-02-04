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
package org.apache.oozie.servlet;

import java.io.IOException;
import java.util.List;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.*;
import org.apache.oozie.client.rest.*;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.CoordRerunXCommand;
import org.apache.oozie.service.BundleEngineService;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.GraphGenerator;
import org.apache.oozie.util.XLog;
import org.json.simple.JSONObject;


@SuppressWarnings("serial")
public class V1JobServlet extends BaseJobServlet {

    private static final String INSTRUMENTATION_NAME = "v1job";
    public static final String COORD_ACTIONS_DEFAULT_LENGTH = "oozie.coord.actions.default.length";

    public V1JobServlet() {
        super(INSTRUMENTATION_NAME);
    }

    /*
     * protected method to start a job
     */
    @Override
    protected void startJob(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        /*
         * Configuration conf = new XConfiguration(request.getInputStream());
         * String wfPath = conf.get(OozieClient.APP_PATH); String coordPath =
         * conf.get(OozieClient.COORDINATOR_APP_PATH);
         *
         * ServletUtilities.ValidateAppPath(wfPath, coordPath);
         */
        String jobId = getResourceName(request);
        if (jobId.endsWith("-W")) {
            startWorkflowJob(request, response);
        }
        else if (jobId.endsWith("-B")) {
            startBundleJob(request, response);
        }
        else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303, RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_START);
        }

    }

    /*
     * protected method to resume a job
     */
    @Override
    protected void resumeJob(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        /*
         * Configuration conf = new XConfiguration(request.getInputStream());
         * String wfPath = conf.get(OozieClient.APP_PATH); String coordPath =
         * conf.get(OozieClient.COORDINATOR_APP_PATH);
         *
         * ServletUtilities.ValidateAppPath(wfPath, coordPath);
         */
        String jobId = getResourceName(request);
        if (jobId.endsWith("-W")) {
            resumeWorkflowJob(request, response);
        }
        else if (jobId.endsWith("-B")) {
            resumeBundleJob(request, response);
        }
        else {
            resumeCoordinatorJob(request, response);
        }
    }

    /*
     * protected method to suspend a job
     */
    @Override
    protected void suspendJob(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        /*
         * Configuration conf = new XConfiguration(request.getInputStream());
         * String wfPath = conf.get(OozieClient.APP_PATH); String coordPath =
         * conf.get(OozieClient.COORDINATOR_APP_PATH);
         *
         * ServletUtilities.ValidateAppPath(wfPath, coordPath);
         */
        String jobId = getResourceName(request);
        if (jobId.endsWith("-W")) {
            suspendWorkflowJob(request, response);
        }
        else if (jobId.endsWith("-B")) {
            suspendBundleJob(request, response);
        }
        else {
            suspendCoordinatorJob(request, response);
        }
    }

    /*
     * protected method to kill a job
     */
    @Override
    protected void killJob(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        /*
         * Configuration conf = new XConfiguration(request.getInputStream());
         * String wfPath = conf.get(OozieClient.APP_PATH); String coordPath =
         * conf.get(OozieClient.COORDINATOR_APP_PATH);
         *
         * ServletUtilities.ValidateAppPath(wfPath, coordPath);
         */
        String jobId = getResourceName(request);
        if (jobId.endsWith("-W")) {
            killWorkflowJob(request, response);
        }
        else if (jobId.endsWith("-B")) {
            killBundleJob(request, response);
        }
        else {
            killCoordinatorJob(request, response);
        }
    }

    /**
     * protected method to change a coordinator job
     * @param request request object
     * @param response response object
     * @throws XServletException
     * @throws IOException
     */
    @Override
    protected void changeJob(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        String jobId = getResourceName(request);
        if (jobId.endsWith("-B")) {
            changeBundleJob(request, response);
        }
        else {
            changeCoordinatorJob(request, response);
        }
    }

    /*
     * protected method to reRun a job
     *
     * @seeorg.apache.oozie.servlet.BaseJobServlet#reRunJob(javax.servlet.http.
     * HttpServletRequest, javax.servlet.http.HttpServletResponse,
     * org.apache.hadoop.conf.Configuration)
     */
    @Override
    protected JSONObject reRunJob(HttpServletRequest request, HttpServletResponse response, Configuration conf)
            throws XServletException, IOException {
        JSONObject json = null;
        String jobId = getResourceName(request);
        if (jobId.endsWith("-W")) {
            reRunWorkflowJob(request, response, conf);
        }
        else if (jobId.endsWith("-B")) {
            rerunBundleJob(request, response, conf);
        }
        else {
            json = reRunCoordinatorActions(request, response, conf);
        }
        return json;
    }

    /*
     * protected method to get a job in JsonBean representation
     */
    @Override
    protected JsonBean getJob(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException, BaseEngineException {
        ServletInputStream is = request.getInputStream();
        byte[] b = new byte[101];
        while (is.readLine(b, 0, 100) != -1) {
            XLog.getLog(getClass()).warn("Printing :" + new String(b));
        }

        JsonBean jobBean = null;
        String jobId = getResourceName(request);
        if (jobId.endsWith("-B")) {
            jobBean = getBundleJob(request, response);
        }
        else {
            if (jobId.endsWith("-W")) {
                jobBean = getWorkflowJob(request, response);
            }
            else {
                if (jobId.contains("-W@")) {
                    jobBean = getWorkflowAction(request, response);
                }
                else {
                    if (jobId.contains("-C@")) {
                        jobBean = getCoordinatorAction(request, response);
                    }
                    else {
                        jobBean = getCoordinatorJob(request, response);
                    }
                }
            }
        }

        return jobBean;
    }

    /*
     * protected method to get a job definition in String format
     */
    @Override
    protected String getJobDefinition(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        String jobDefinition = null;
        String jobId = getResourceName(request);
        if (jobId.endsWith("-W")) {
            jobDefinition = getWorkflowJobDefinition(request, response);
        }
        else if (jobId.endsWith("-B")) {
            jobDefinition = getBundleJobDefinition(request, response);
        }
        else {
            jobDefinition = getCoordinatorJobDefinition(request, response);
        }
        return jobDefinition;
    }

    /*
     * protected method to stream a job log into response object
     */
    @Override
    protected void streamJobLog(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        String jobId = getResourceName(request);
        if (jobId.endsWith("-W")) {
            streamWorkflowJobLog(request, response);
        }
        else if (jobId.endsWith("-B")) {
            streamBundleJob(request, response);
        }
        else {
            streamCoordinatorJobLog(request, response);
        }
    }

    @Override
    protected void streamJobGraph(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        String jobId = getResourceName(request);
        if (jobId.endsWith("-W")) {
            // Applicable only to worflow, for now
            response.setContentType(RestConstants.PNG_IMAGE_CONTENT_TYPE);
            try {
                String showKill = request.getParameter(RestConstants.JOB_SHOW_KILL_PARAM);
                boolean sK = showKill != null && (showKill.equalsIgnoreCase("yes") || showKill.equals("1") || showKill.equalsIgnoreCase("true"));

                new GraphGenerator(
                        getWorkflowJobDefinition(request, response),
                        (JsonWorkflowJob)getWorkflowJob(request, response),
                        sK).write(response.getOutputStream());
            }
            catch (Exception e) {
                throw new XServletException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ErrorCode.E0307, e.getMessage(), e);
            }
        }
        else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0306);
        }
    }

    /**
     * Start wf job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void startWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                getAuthToken(request));

        String jobId = getResourceName(request);
        try {
            dagEngine.start(jobId);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Start bundle job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void startBundleJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request),
                getAuthToken(request));
        String jobId = getResourceName(request);
        try {
            bundleEngine.start(jobId);
        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Resume workflow job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void resumeWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                getAuthToken(request));

        String jobId = getResourceName(request);
        try {
            dagEngine.resume(jobId);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Resume bundle job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void resumeBundleJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request),
                getAuthToken(request));
        String jobId = getResourceName(request);
        try {
            bundleEngine.resume(jobId);
        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Resume coordinator job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     * @throws CoordinatorEngineException
     */
    private void resumeCoordinatorJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        String jobId = getResourceName(request);
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request), getAuthToken(request));
        try {
            coordEngine.resume(jobId);
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Suspend a wf job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void suspendWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                getAuthToken(request));

        String jobId = getResourceName(request);
        try {
            dagEngine.suspend(jobId);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Suspend bundle job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void suspendBundleJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request),
                getAuthToken(request));
        String jobId = getResourceName(request);
        try {
            bundleEngine.suspend(jobId);
        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Suspend coordinator job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void suspendCoordinatorJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request), getAuthToken(request));
        String jobId = getResourceName(request);
        try {
            coordEngine.suspend(jobId);
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Kill a wf job
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void killWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                getAuthToken(request));

        String jobId = getResourceName(request);
        try {
            dagEngine.kill(jobId);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Kill a coord job
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void killCoordinatorJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request), getAuthToken(request));
        String jobId = getResourceName(request);
        try {
            coordEngine.kill(jobId);
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Kill bundle job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void killBundleJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request),
                getAuthToken(request));
        String jobId = getResourceName(request);
        try {
            bundleEngine.kill(jobId);
        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Change a coordinator job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void changeCoordinatorJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request), getAuthToken(request));
        String jobId = getResourceName(request);
        String changeValue = request.getParameter(RestConstants.JOB_CHANGE_VALUE);
        try {
            coordEngine.change(jobId, changeValue);
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Change a bundle job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void changeBundleJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(
                getUser(request), getAuthToken(request));
        String jobId = getResourceName(request);
        String changeValue = request.getParameter(RestConstants.JOB_CHANGE_VALUE);
        try {
            bundleEngine.change(jobId, changeValue);
        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Rerun a wf job
     *
     * @param request servlet request
     * @param response servlet response
     * @param conf configuration object
     * @throws XServletException
     */
    private void reRunWorkflowJob(HttpServletRequest request, HttpServletResponse response, Configuration conf)
            throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                getAuthToken(request));

        String jobId = getResourceName(request);
        try {
            dagEngine.reRun(jobId, conf);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Rerun bundle job
     *
     * @param request servlet request
     * @param response servlet response
     * @param conf configration object
     * @throws XServletException
     */
    private void rerunBundleJob(HttpServletRequest request, HttpServletResponse response, Configuration conf)
            throws XServletException {
        JSONObject json = new JSONObject();
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request),
                getAuthToken(request));
        String jobId = getResourceName(request);

        String coordScope = request.getParameter(RestConstants.JOB_BUNDLE_RERUN_COORD_SCOPE_PARAM);
        String dateScope = request.getParameter(RestConstants.JOB_BUNDLE_RERUN_DATE_SCOPE_PARAM);
        String refresh = request.getParameter(RestConstants.JOB_COORD_RERUN_REFRESH_PARAM);
        String noCleanup = request.getParameter(RestConstants.JOB_COORD_RERUN_NOCLEANUP_PARAM);

        XLog.getLog(getClass()).info(
                "Rerun Bundle for jobId=" + jobId + ", coordScope=" + coordScope + ", dateScope=" + dateScope + ", refresh="
                        + refresh + ", noCleanup=" + noCleanup);

        try {
            bundleEngine.reRun(jobId, coordScope, dateScope, Boolean.valueOf(refresh), Boolean.valueOf(noCleanup));
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Rerun coordinator actions
     *
     * @param request servlet request
     * @param response servlet response
     * @param conf configuration object
     * @throws XServletException
     */
    @SuppressWarnings("unchecked")
    private JSONObject reRunCoordinatorActions(HttpServletRequest request, HttpServletResponse response,
            Configuration conf) throws XServletException {
        JSONObject json = new JSONObject();
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(getUser(request),
                getAuthToken(request));

        String jobId = getResourceName(request);

        String rerunType = request.getParameter(RestConstants.JOB_COORD_RERUN_TYPE_PARAM);
        String scope = request.getParameter(RestConstants.JOB_COORD_RERUN_SCOPE_PARAM);
        String refresh = request.getParameter(RestConstants.JOB_COORD_RERUN_REFRESH_PARAM);
        String noCleanup = request.getParameter(RestConstants.JOB_COORD_RERUN_NOCLEANUP_PARAM);

        XLog.getLog(getClass()).info(
                "Rerun coordinator for jobId=" + jobId + ", rerunType=" + rerunType + ",scope=" + scope + ",refresh="
                        + refresh + ", noCleanup=" + noCleanup);

        try {
            if (!(rerunType.equals(RestConstants.JOB_COORD_RERUN_DATE) || rerunType
                    .equals(RestConstants.JOB_COORD_RERUN_ACTION))) {
                throw new CommandException(ErrorCode.E1018, "date or action expected.");
            }
            CoordinatorActionInfo coordInfo = coordEngine.reRun(jobId, rerunType, scope, Boolean.valueOf(refresh),
                    Boolean.valueOf(noCleanup));
            List<CoordinatorActionBean> coordActions;
            if (coordInfo != null) {
                coordActions = coordInfo.getCoordActions();
            }
            else {
                coordActions = CoordRerunXCommand.getCoordActions(rerunType, jobId, scope);
            }
            json.put(JsonTags.COORDINATOR_ACTIONS, CoordinatorActionBean.toJSONArray(coordActions, "GMT"));
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        catch (CommandException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }



    /**
     * Get workflow job
     *
     * @param request servlet request
     * @param response servlet response
     * @return JsonBean WorkflowJobBean
     * @throws XServletException
     */
    private JsonBean getWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        JsonBean jobBean = null;
        String jobId = getResourceName(request);
        String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
        String lenStr = request.getParameter(RestConstants.LEN_PARAM);
        int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
        start = (start < 1) ? 1 : start;
        int len = (lenStr != null) ? Integer.parseInt(lenStr) : 0;
        len = (len < 1) ? Integer.MAX_VALUE : len;
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                getAuthToken(request));
        try {
            jobBean = (JsonBean) dagEngine.getJob(jobId, start, len);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return jobBean;
    }

    /**
     * Get wf action info
     *
     * @param request servlet request
     * @param response servlet response
     * @return JsonBean WorkflowActionBean
     * @throws XServletException
     */
    private JsonBean getWorkflowAction(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                getAuthToken(request));

        JsonBean actionBean = null;
        String actionId = getResourceName(request);
        try {
            actionBean = dagEngine.getWorkflowAction(actionId);
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return actionBean;
    }

    /**
     * Get coord job info
     *
     * @param request servlet request
     * @param response servlet response
     * @return JsonBean CoordinatorJobBean
     * @throws XServletException
     * @throws BaseEngineException
     */
    private JsonBean getCoordinatorJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, BaseEngineException {
        JsonBean jobBean = null;
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request), getAuthToken(request));
        String jobId = getResourceName(request);
        String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
        String lenStr = request.getParameter(RestConstants.LEN_PARAM);
        String filter = request.getParameter(RestConstants.JOB_FILTER_PARAM);
        int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
        start = (start < 1) ? 1 : start;
        // Get default number of coordinator actions to be retrieved
        int defaultLen = Services.get().getConf().getInt(COORD_ACTIONS_DEFAULT_LENGTH, 1000);
        int len = (lenStr != null) ? Integer.parseInt(lenStr) : 0;
        len = (len < 1) ? defaultLen : len;
        try {
            JsonCoordinatorJob coordJob = coordEngine.getCoordJob(jobId, filter, start, len);
            jobBean = coordJob;
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return jobBean;
    }

    /**
     * Get bundle job info
     *
     * @param request servlet request
     * @param response servlet response
     * @return JsonBean bundle job bean
     * @throws XServletException
     * @throws BaseEngineException
     */
    private JsonBean getBundleJob(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            BaseEngineException {
        JsonBean jobBean = null;
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request),
                getAuthToken(request));
        String jobId = getResourceName(request);

        try {
            jobBean = (JsonBean) bundleEngine.getBundleJob(jobId);

            return jobBean;
        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Get coordinator action
     *
     * @param request servlet request
     * @param response servlet response
     * @return JsonBean CoordinatorActionBean
     * @throws XServletException
     * @throws BaseEngineException
     */
    private JsonBean getCoordinatorAction(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, BaseEngineException {
        JsonBean actionBean = null;
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request), getAuthToken(request));
        String actionId = getResourceName(request);
        try {
            actionBean = coordEngine.getCoordAction(actionId);
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return actionBean;
    }

    /**
     * Get wf job definition
     *
     * @param request servlet request
     * @param response servlet response
     * @return String wf definition
     * @throws XServletException
     */
    private String getWorkflowJobDefinition(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                getAuthToken(request));

        String wfDefinition;
        String jobId = getResourceName(request);
        try {
            wfDefinition = dagEngine.getDefinition(jobId);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return wfDefinition;
    }

    /**
     * Get bundle job definition
     *
     * @param request servlet request
     * @param response servlet response
     * @return String bundle definition
     * @throws XServletException
     */
    private String getBundleJobDefinition(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request),
                getAuthToken(request));
        String bundleDefinition;
        String jobId = getResourceName(request);
        try {
            bundleDefinition = bundleEngine.getDefinition(jobId);
        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return bundleDefinition;
    }

    /**
     * Get coordinator job definition
     *
     * @param request servlet request
     * @param response servlet response
     * @return String coord definition
     * @throws XServletException
     */
    private String getCoordinatorJobDefinition(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {

        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request), getAuthToken(request));

        String jobId = getResourceName(request);

        String coordDefinition = null;
        try {
            coordDefinition = coordEngine.getDefinition(jobId);
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return coordDefinition;
    }

    /**
     * Stream wf job log
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     * @throws IOException
     */
    private void streamWorkflowJobLog(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request),
                getAuthToken(request));
        String jobId = getResourceName(request);
        try {
            dagEngine.streamLog(jobId, response.getWriter());
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Stream bundle job log
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     */
    private void streamBundleJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request),
                getAuthToken(request));
        String jobId = getResourceName(request);
        try {
            bundleEngine.streamLog(jobId, response.getWriter());
        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Stream coordinator job log
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException
     * @throws IOException
     */
    private void streamCoordinatorJobLog(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {

        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request), getAuthToken(request));
        String jobId = getResourceName(request);
        String logRetrievalScope = request.getParameter(RestConstants.JOB_LOG_SCOPE_PARAM);
        String logRetrievalType = request.getParameter(RestConstants.JOB_LOG_TYPE_PARAM);
        try {
            coordEngine.streamLog(jobId, logRetrievalScope, logRetrievalType, response.getWriter());
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        catch (CommandException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }
}
