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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BaseEngineException;
import org.apache.oozie.BundleEngine;
import org.apache.oozie.BundleEngineException;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonUtils;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.service.BundleEngineService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.ParameterVerifierException;
import org.apache.oozie.util.graph.GraphGenerator;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.graph.GraphRenderer;
import org.apache.oozie.util.graph.GraphvizRenderer;
import org.apache.oozie.util.graph.OutputFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@SuppressWarnings("serial")
public class V1JobServlet extends BaseJobServlet {

    private static final String INSTRUMENTATION_NAME = "v1job";
    public static final String COORD_ACTIONS_DEFAULT_LENGTH = "oozie.coord.actions.default.length";

    final static String NOT_SUPPORTED_MESSAGE = "Not supported in v1";

    public V1JobServlet() {
        super(INSTRUMENTATION_NAME);
    }

    protected V1JobServlet(String instrumentation_name){
        super(instrumentation_name);
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
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303, RestConstants.ACTION_PARAM,
                    RestConstants.JOB_ACTION_START);
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
    protected JSONObject killJob(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        /*
         * Configuration conf = new XConfiguration(request.getInputStream());
         * String wfPath = conf.get(OozieClient.APP_PATH); String coordPath =
         * conf.get(OozieClient.COORDINATOR_APP_PATH);
         *
         * ServletUtilities.ValidateAppPath(wfPath, coordPath);
         */
        String jobId = getResourceName(request);
        JSONObject json = null;
        if (jobId.endsWith("-W")) {
            killWorkflowJob(request, response);
        }
        else if (jobId.endsWith("-B")) {
            killBundleJob(request, response);
        }
        else {
            json = killCoordinator(request, response);
        }
        return json;
    }

    /**
     * protected method to change a coordinator job
     * @param request request object
     * @param response response object
     * @throws XServletException in case if BundleEngineException or CoordinatorEngineException occurs
     * @throws IOException in case of parsing error
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
    @Override
    protected JSONObject ignoreJob(HttpServletRequest request, HttpServletResponse response) throws XServletException, IOException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, NOT_SUPPORTED_MESSAGE);
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
            XLog.getLog(getClass()).warn("Printing :" + new String(b, StandardCharsets.UTF_8));
        }

        JsonBean jobBean;
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
        try {
            String jobId = getResourceName(request);
            if (jobId.endsWith("-W")) {
                streamWorkflowJobLog(request, response);
            }
            else if (jobId.endsWith("-B")) {
                streamBundleJobLog(request, response);
            }
            else {
                streamCoordinatorJobLog(request, response);
            }
        }
        catch (Exception e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0307, e.getMessage());
        }
    }

    @Override
    protected void streamJobGraph(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        String jobId = getResourceName(request);
        if (jobId.endsWith("-W")) {
            try {

                final String showKillParameter = request.getParameter(RestConstants.JOB_SHOW_KILL_PARAM);
                final boolean showKill = isShowKillSet(showKillParameter);

                final String formatParameter = request.getParameter(RestConstants.JOB_FORMAT_PARAM);
                final OutputFormat outputFormat = getOutputFormat(formatParameter);

                final String contentType = getContentType(outputFormat);

                response.setContentType(contentType);

                final Instrumentation.Cron cron = new Instrumentation.Cron();
                cron.start();

                final GraphRenderer graphRenderer = new GraphvizRenderer();

                new GraphGenerator(
                            getWorkflowJobDefinition(request, response),
                            (WorkflowJobBean)getWorkflowJob(request, response),
                            showKill,
                            graphRenderer).write(response.getOutputStream(), outputFormat);

                cron.stop();
                instrument(outputFormat, cron);
            }
            catch (final Exception e) {
                throw new XServletException(HttpServletResponse.SC_NOT_FOUND, ErrorCode.E0307, e.getMessage(), e);
            }
        }
        else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0306);
        }
    }

    private boolean isShowKillSet(final String showKillParameter) {
        return showKillParameter != null &&
                (showKillParameter.equalsIgnoreCase("yes") ||
                        showKillParameter.equals("1") ||
                        showKillParameter.equalsIgnoreCase("true"));
    }

    private OutputFormat getOutputFormat(final String formatParameter) {
        final OutputFormat outputFormat;
        if (Strings.isNullOrEmpty(formatParameter)) {
            outputFormat = OutputFormat.PNG;
        }
        else {
            outputFormat = OutputFormat.valueOf(formatParameter.toUpperCase(Locale.getDefault()));
        }
        return outputFormat;
    }

    private String getContentType(final OutputFormat outputFormat) {
        final String contentType;

        switch (outputFormat) {
            case PNG:
                contentType = RestConstants.PNG_IMAGE_CONTENT_TYPE;
                break;
            case DOT:
                contentType = RestConstants.TEXT_CONTENT_TYPE;
                break;
            case SVG:
                contentType = RestConstants.SVG_IMAGE_CONTENT_TYPE;
                break;
            default:
                throw new IllegalArgumentException("Unknown output format, cannot get content type: " + outputFormat);
        }

        return contentType;
    }

    private void instrument(final OutputFormat outputFormat, final Instrumentation.Cron cron) {
        addCron(INSTRUMENTATION_NAME + "-graph", cron);
        incrCounter(INSTRUMENTATION_NAME + "-graph", 1);
        addCron(INSTRUMENTATION_NAME + "-graph-" + outputFormat.toString().toLowerCase(Locale.getDefault()), cron);
        incrCounter(INSTRUMENTATION_NAME + "-graph-" + outputFormat.toString().toLowerCase(Locale.getDefault()), 1);
    }

    /**
     * Start wf job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException in case if DagEngineException occurs
     */
    private void startWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));

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
     * @throws XServletException in case if BundleEngineException occurs
     */
    private void startBundleJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
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
     * @throws XServletException in case if DagEngineException occurs
     */
    private void resumeWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));

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
     * @throws XServletException in case if BundleEngineException occurs
     */
    private void resumeBundleJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
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
     * @throws XServletException in case if CoordinatorEngineException occurs
     */
    private void resumeCoordinatorJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        String jobId = getResourceName(request);
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request));
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
     * @throws XServletException in case if DagEngineException occurs
     */
    private void suspendWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));

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
     * @throws XServletException in case if BundleEngineException occurs
     */
    private void suspendBundleJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
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
     * @throws XServletException in case if CoordinatorEngineException occurs
     */
    private void suspendCoordinatorJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request));
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
     * @throws XServletException in case if DagEngineException occurs
     */
    private void killWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));

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
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException in case if CoordinatorEngineException or CommandException occurs
     */
    @SuppressWarnings("unchecked")
    private JSONObject killCoordinator(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        String jobId = getResourceName(request);
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class)
                .getCoordinatorEngine(getUser(request));
        JSONObject json = null;
        String rangeType = request.getParameter(RestConstants.JOB_COORD_RANGE_TYPE_PARAM);
        String scope = request.getParameter(RestConstants.JOB_COORD_SCOPE_PARAM);

        validateScopeSize(scope, rangeType);

        try {
            if (rangeType != null && scope != null) {
                XLog.getLog(getClass()).info(
                        "Kill coordinator actions for jobId=" + jobId + ", rangeType=" + rangeType + ",scope=" + scope);

                json = new JSONObject();
                CoordinatorActionInfo coordInfo = coordEngine.killActions(jobId, rangeType, scope);
                List<CoordinatorActionBean> coordActions;
                if (coordInfo != null) {
                    coordActions = coordInfo.getCoordActions();
                }
                else {
                    coordActions = CoordUtils.getCoordActions(rangeType, jobId, scope, true);
                }
                json.put(JsonTags.COORDINATOR_ACTIONS, CoordinatorActionBean.toJSONArray(coordActions, "GMT"));
            }
            else {
                coordEngine.kill(jobId);
            }
        }
        catch (CoordinatorEngineException | CommandException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return json;
    }

    /**
     * Kill bundle job
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException in case if BundleEngineException occurs
     */
    private void killBundleJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
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
     * @throws XServletException in case if CoordinatorEngineException occurs
     */
    private void changeCoordinatorJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request));
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
     * @throws XServletException in case if BundleEngineException occurs
     */
    private void changeBundleJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
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
     * @throws XServletException in case if DagEngineException occurs
     */
    private void reRunWorkflowJob(HttpServletRequest request, HttpServletResponse response, Configuration conf)
            throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));

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
     * @throws XServletException in case if BaseEngineException occurs
     */
    private void rerunBundleJob(HttpServletRequest request, HttpServletResponse response, Configuration conf)
            throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
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
     * @throws XServletException in case if BaseEngineException or CommandException occurs
     */
    @SuppressWarnings("unchecked")
    private JSONObject reRunCoordinatorActions(HttpServletRequest request, HttpServletResponse response,
            Configuration conf) throws XServletException {
        JSONObject json = new JSONObject();
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(getUser(request));

        String jobId = getResourceName(request);

        String rerunType = request.getParameter(RestConstants.JOB_COORD_RANGE_TYPE_PARAM);
        String scope = request.getParameter(RestConstants.JOB_COORD_SCOPE_PARAM);
        String refresh = request.getParameter(RestConstants.JOB_COORD_RERUN_REFRESH_PARAM);
        String noCleanup = request.getParameter(RestConstants.JOB_COORD_RERUN_NOCLEANUP_PARAM);
        String failed = request.getParameter(RestConstants.JOB_COORD_RERUN_FAILED_PARAM);

        XLog.getLog(getClass()).info(
                "Rerun coordinator for jobId=" + jobId + ", rerunType=" + rerunType + ",scope=" + scope + ",refresh="
                        + refresh + ", noCleanup=" + noCleanup);

        validateScopeSize(scope, rerunType);

        try {
            if (!(rerunType.equals(RestConstants.JOB_COORD_SCOPE_DATE) || rerunType
                    .equals(RestConstants.JOB_COORD_SCOPE_ACTION))) {
                throw new CommandException(ErrorCode.E1018, "date or action expected.");
            }
            CoordinatorActionInfo coordInfo = coordEngine.reRun(jobId, rerunType, scope, Boolean.valueOf(refresh),
                    Boolean.valueOf(noCleanup), Boolean.valueOf(failed), conf);
            List<CoordinatorActionBean> coordActions;
            if (coordInfo != null) {
                coordActions = coordInfo.getCoordActions();
            }
            else {
                coordActions = CoordUtils.getCoordActions(rerunType, jobId, scope, false);
            }
            json.put(JsonTags.COORDINATOR_ACTIONS, CoordinatorActionBean.toJSONArray(coordActions, "GMT"));
        }
        catch (BaseEngineException | CommandException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * Validates if the number of elements defined in 'scope' (comma separated list of values and ranges)
     * is less than or equal than the maximum allowed count: oozie.coord.actions.scope.max.size.
     *
     * @param scope comma separated list of values and ranges
     * @throws XServletException if there are too many elements or ranges/values are invalid
     */
    static void validateScopeSize(final String scope, final String rangeType) throws XServletException {
        if (!"action".equalsIgnoreCase(StringUtils.stripToNull(rangeType))) {
            return;
        }

        final int maxElemCount = ConfigurationService.getInt("oozie.coord.actions.scope.max.size");

        try {
            final int elemCountOfRanges = CoordUtils.getElemCountOfRanges(CoordUtils.parseScopeToRanges(scope));
            if (elemCountOfRanges > maxElemCount) {
                throw new ParameterVerifierException(
                        ErrorCode.E0309,
                        "scope",
                        scope,
                        String.format(
                                "too many elements are requested: %s, maximum allowed: %s",
                                elemCountOfRanges,
                                maxElemCount
                        )
                );
            }
        } catch (final XException e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, e);
        }
    }

    /**
     * Get workflow job
     *
     * @param request servlet request
     * @param response servlet response
     * @return JsonBean WorkflowJobBean
     * @throws XServletException in case if DagEngineException occurs
     */
    protected JsonBean getWorkflowJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        JsonBean jobBean = getWorkflowJobBean(request, response);
        // for backward compatibility (OOZIE-1231)
        swapMRActionID((WorkflowJob)jobBean);
        return jobBean;
    }

    /**
     * Get workflow job
     *
     * @param request servlet request
     * @param response servlet response
     * @return JsonBean WorkflowJobBean
     * @throws XServletException in case if DagEngineException occurs
     */
    protected JsonBean getWorkflowJobBean(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        JsonBean jobBean;
        String jobId = getResourceName(request);
        String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
        String lenStr = request.getParameter(RestConstants.LEN_PARAM);
        int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
        start = (start < 1) ? 1 : start;
        int len = (lenStr != null) ? Integer.parseInt(lenStr) : 0;
        len = (len < 1) ? Integer.MAX_VALUE : len;
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));
        try {
            jobBean = (JsonBean) dagEngine.getJob(jobId, start, len);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return jobBean;
    }

    private void swapMRActionID(WorkflowJob wjBean) {
        List<WorkflowAction> actions = wjBean.getActions();
        if (actions != null) {
            for (WorkflowAction wa : actions) {
                swapMRActionID(wa);
            }
        }
    }

    private void swapMRActionID(WorkflowAction waBean) {
        if (waBean.getType().equals("map-reduce")) {
            String childId = waBean.getExternalChildIDs();
            if (childId != null && !childId.equals("")) {
                String consoleBase = getConsoleBase(waBean.getConsoleUrl());
                ((WorkflowActionBean) waBean).setConsoleUrl(consoleBase + childId);
                ((WorkflowActionBean) waBean).setExternalId(childId);
                ((WorkflowActionBean) waBean).setExternalChildIDs("");
            }
        }
    }

    private String getConsoleBase(String url) {
        String consoleBase;
        if (url.indexOf("application") != -1) {
            consoleBase = url.split("application_[0-9]+_[0-9]+")[0];
        }
        else {
            consoleBase = url.split("job_[0-9]+_[0-9]+")[0];
        }
        return consoleBase;
    }

    /**
     * Get wf action info
     *
     * @param request servlet request
     * @param response servlet response
     * @return JsonBean WorkflowActionBean
     * @throws XServletException in case if BaseEngineException occurs
     */
    protected JsonBean getWorkflowAction(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        JsonBean actionBean = getWorkflowActionBean(request, response);
        // for backward compatibility (OOZIE-1231)
        swapMRActionID((WorkflowAction)actionBean);
        return actionBean;
    }

    protected JsonBean getWorkflowActionBean(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));

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
     * @throws XServletException in case if CoordinatorEngineException occurs
     * @throws BaseEngineException if CommandException occurs
     */
    protected JsonBean getCoordinatorJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, BaseEngineException {
        JsonBean jobBean;
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request));
        String jobId = getResourceName(request);
        String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
        String lenStr = request.getParameter(RestConstants.LEN_PARAM);
        String filter = request.getParameter(RestConstants.JOB_FILTER_PARAM);
        String orderStr = request.getParameter(RestConstants.ORDER_PARAM);
        boolean order = (orderStr != null && orderStr.equals("desc")) ? true : false;
        int offset = (startStr != null) ? Integer.parseInt(startStr) : 1;
        offset = (offset < 1) ? 1 : offset;
        // Get default number of coordinator actions to be retrieved
        int defaultLen = ConfigurationService.getInt(COORD_ACTIONS_DEFAULT_LENGTH);
        int len = (lenStr != null) ? Integer.parseInt(lenStr) : 0;
        len = getCoordinatorJobLength(defaultLen, len);
        try {
            CoordinatorJobBean coordJob = coordEngine.getCoordJob(jobId, filter, offset, len, order);
            jobBean = coordJob;
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return jobBean;
    }

    /**
     * Given the requested length and the default length, determine how many coordinator jobs to return.
     * Used by {@link #getCoordinatorJob(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)}
     *
     * @param defaultLen The default length
     * @param len The requested length
     * @return The length to use
     */
    protected int getCoordinatorJobLength(int defaultLen, int len) {
        return (len < 1) ? defaultLen : len;
    }

    /**
     * Get bundle job info
     *
     * @param request servlet request
     * @param response servlet response
     * @return JsonBean bundle job bean
     * @throws XServletException in case if BundleEngineException occurs
     */
    private JsonBean getBundleJob(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        JsonBean jobBean;
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
        String jobId = getResourceName(request);

        try {
            jobBean = bundleEngine.getBundleJob(jobId);

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
     * @throws XServletException in case if CoordinatorEngineException occurs
     * @throws BaseEngineException if CommandException occurs
     */
    private JsonBean getCoordinatorAction(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, BaseEngineException {
        JsonBean actionBean;
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request));
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
     * @throws XServletException in case if DagEngineException occurs
     */
    private String getWorkflowJobDefinition(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));

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
     * @throws XServletException in case if BundleEngineException occurs
     */
    private String getBundleJobDefinition(HttpServletRequest request, HttpServletResponse response) throws XServletException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
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
     * @throws XServletException in case if BaseEngineException occurs
     */
    private String getCoordinatorJobDefinition(HttpServletRequest request, HttpServletResponse response)
            throws XServletException {

        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request));

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
     * @throws XServletException in case if BaseEngineException occurs
     * @throws IOException in case of parsing error
     */
    private void streamWorkflowJobLog(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));
        String jobId = getResourceName(request);
        try {
            dagEngine.streamLog(jobId, response.getWriter(), request.getParameterMap());
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Stream bundle job log
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException in case if BaseEngineException occurs
     */
    private void streamBundleJobLog(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
        String jobId = getResourceName(request);
        try {
            bundleEngine.streamLog(jobId, response.getWriter(), request.getParameterMap());
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    /**
     * Stream coordinator job log
     *
     * @param request servlet request
     * @param response servlet response
     * @throws XServletException in case if BaseEngineException or CommandException occurs
     * @throws IOException in case of parsing error
     */
    private void streamCoordinatorJobLog(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {

        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request));
        String jobId = getResourceName(request);
        String logRetrievalScope = request.getParameter(RestConstants.JOB_LOG_SCOPE_PARAM);
        String logRetrievalType = request.getParameter(RestConstants.JOB_LOG_TYPE_PARAM);
        try {
            coordEngine.streamLog(jobId, logRetrievalScope, logRetrievalType, response.getWriter(), request.getParameterMap());
        }
        catch (BaseEngineException | CommandException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }

    @Override
    protected String getJMSTopicName(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, NOT_SUPPORTED_MESSAGE);
    }

    @Override
    protected JSONObject getJobsByParentId(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        JSONObject json = new JSONObject();
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class)
                .getCoordinatorEngine(getUser(request));
        String coordActionId;
        String type = request.getParameter(RestConstants.JOB_COORD_RANGE_TYPE_PARAM);
        String scope = request.getParameter(RestConstants.JOB_COORD_SCOPE_PARAM);

        validateScopeSize(scope, type);

        // for getting allruns for coordinator action - 2 alternate endpoints
        if (type != null && type.equals(RestConstants.JOB_COORD_SCOPE_ACTION) && scope != null) {
            // endpoint - oozie/v2/coord-job-id?type=action&scope=action-num&show=allruns
            String jobId = getResourceName(request);
            coordActionId = Services.get().get(UUIDService.class).generateChildId(jobId, scope);
        }
        else {
            // endpoint - oozie/v2/coord-action-id?show=allruns
            coordActionId = getResourceName(request);
        }
        try {
            List<WorkflowJobBean> wfs = coordEngine.getReruns(coordActionId);
            JSONArray array = new JSONArray();
            if (wfs != null) {
                for (WorkflowJobBean wf : wfs) {
                    JSONObject json1 = new JSONObject();
                    json1.put(JsonTags.WORKFLOW_ID, wf.getId());
                    json1.put(JsonTags.WORKFLOW_STATUS, wf.getStatus().toString());
                    json1.put(JsonTags.WORKFLOW_START_TIME, JsonUtils.formatDateRfc822(wf.getStartTime(), "GMT"));
                    json1.put(JsonTags.WORKFLOW_ACTION_END_TIME, JsonUtils.formatDateRfc822(wf.getEndTime(), "GMT"));
                    array.add(json1);
                }
            }
            json.put(JsonTags.WORKFLOWS_JOBS, array);
            return json;
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
    }
    /**
     * not supported for v1
     */
    @Override
    protected JSONObject updateJob(HttpServletRequest request, HttpServletResponse response, Configuration conf)
            throws XServletException, IOException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, NOT_SUPPORTED_MESSAGE);
    }

    @Override
    protected String getJobStatus(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, NOT_SUPPORTED_MESSAGE);
    }

    @Override
    protected void streamJobErrorLog(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, NOT_SUPPORTED_MESSAGE);
    }
    @Override
    protected void streamJobAuditLog(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, NOT_SUPPORTED_MESSAGE);
    }
    @Override
    void slaEnableAlert(HttpServletRequest request, HttpServletResponse response) throws XServletException, IOException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, NOT_SUPPORTED_MESSAGE);
    }

    @Override
    void slaDisableAlert(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, NOT_SUPPORTED_MESSAGE);
    }

    @Override
    void slaChange(HttpServletRequest request, HttpServletResponse response) throws XServletException, IOException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, NOT_SUPPORTED_MESSAGE);
    }

    @Override
    JSONObject getCoordActionMissingDependencies(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, NOT_SUPPORTED_MESSAGE);
    }

    @Override
    JSONArray getActionRetries(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, NOT_SUPPORTED_MESSAGE);
    }
}