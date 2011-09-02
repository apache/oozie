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
package org.apache.oozie.servlet;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BaseEngineException;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonCoordinatorJob;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;
import org.json.simple.JSONObject;

@SuppressWarnings("serial")
public class V1JobServlet extends BaseJobServlet {

    private static final String INSTRUMENTATION_NAME = "v1job";

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
        else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303);
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
    protected void changeJob(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        changeCoordinatorJob(request, response);
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
        /*
         * Configuration conf = new XConfiguration(request.getInputStream());
         * String wfPath = conf.get(OozieClient.APP_PATH); String coordPath =
         * conf.get(OozieClient.COORDINATOR_APP_PATH);
         *
         * ServletUtilities.ValidateAppPath(wfPath, coordPath);
         */
        JsonBean jobBean = null;
        String jobId = getResourceName(request);
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
                    // jobBean = new JsonCoordinatorJob(getCoordinatorJob(request, response));
                    jobBean = getCoordinatorJob(request, response);
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
        else {
            streamCoordinatorJobLog(request, response);
        }
    }

    /**
     * @param request
     * @param response
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
     * @param request
     * @param response
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
     * @param request
     * @param response
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
     * @param request
     * @param response
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
     * @param request
     * @param response
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
     * @param request
     * @param response
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
     * @param request
     * @param response
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
     * Rerun workflow job
     *
     * @param request
     * @param response
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
     * @param request
     * @param response
     * @param conf
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
     * Rerun coordinator actions
     *
     * @param request
     * @param response
     * @param conf
     * @throws XServletException
     */
    @SuppressWarnings("unchecked")
    private JSONObject reRunCoordinatorActions(HttpServletRequest request, HttpServletResponse response,
            Configuration conf)
            throws XServletException {
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
            CoordinatorActionInfo coordInfo = coordEngine.reRun(jobId, rerunType, scope, Boolean.valueOf(refresh),
                    Boolean.valueOf(noCleanup));
            List<CoordinatorActionBean> actions = coordInfo.getCoordActions();
            json.put(JsonTags.COORDINATOR_ACTIONS, CoordinatorActionBean.toJSONArray(actions));
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * Get workflow job
     *
     * @param request
     * @param response
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
     * @param request
     * @param response
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
     * @param request
     * @param response
     * @return JsonBean CoordinatorJobBean
     * @throws XServletException
     * @throws BaseEngineException
     */
    //private JSONObject getCoordinatorJob(HttpServletRequest request, HttpServletResponse response)
    private JsonBean getCoordinatorJob(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, BaseEngineException {
        JsonBean jobBean = null;
        // JSONObject json = new JSONObject();
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request), getAuthToken(request));
        String jobId = getResourceName(request);
        String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
        String lenStr = request.getParameter(RestConstants.LEN_PARAM);
        int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
        start = (start < 1) ? 1 : start;
        int len = (lenStr != null) ? Integer.parseInt(lenStr) : 0;
        len = (len < 1) ? Integer.MAX_VALUE : len;
        try {
            JsonCoordinatorJob coordJob = coordEngine.getCoordJob(jobId, start, len);
            // coordJob.setOffset(start);
            // coordJob.setLen(len);
            jobBean = coordJob;
            // jobBean = (JsonBean) coordEngine.getCoordJob(jobId, start, len);
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return jobBean;
        //return json;
    }

    /**
     * @param request
     * @param response
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
     * @param request
     * @param response
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
     * @param request
     * @param response
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
     * @param request
     * @param response
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
     * @param request
     * @param response
     * @throws XServletException
     * @throws IOException
     */
    private void streamCoordinatorJobLog(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {

        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                getUser(request), getAuthToken(request));

        String jobId = getResourceName(request);

        try {
            coordEngine.streamLog(jobId, response.getWriter());
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

    }
}
