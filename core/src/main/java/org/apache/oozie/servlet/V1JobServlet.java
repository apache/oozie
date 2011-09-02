/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.servlet;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BaseEngineException;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonCoordinatorJob;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XConfiguration;
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

    /*
     * protected method to reRun a job
     */
    protected void reRunJob(HttpServletRequest request, HttpServletResponse response, Configuration conf)
            throws XServletException, IOException {
        /*
         * String wfPath = conf.get(OozieClient.APP_PATH); String coordPath =
         * conf.get(OozieClient.COORDINATOR_APP_PATH);
         * 
         * ServletUtilities.ValidateAppPath(wfPath, coordPath);
         */
        String jobId = getResourceName(request);
        if (jobId.endsWith("-W")) {
            reRunWorkflowJob(request, response, conf);
        }
        else {
            reRunCoordinatorJob(request, response, conf);
        }
    }

    /*
     * protected method to get a job in JsonBean representation
     */
    protected JsonBean getJob(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException, BaseEngineException {
        ServletInputStream is = request.getInputStream();
        byte[] b = new byte[101];
        while (is.readLine(b, 0, 100) != -1) {
            XLog.getLog(getClass()).warn("PRinting :" + new String(b));
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
     * @param request
     * @param response
     * @param conf
     * @throws XServletException
     */
    private void reRunCoordinatorJob(HttpServletRequest request, HttpServletResponse response, Configuration conf)
            throws XServletException {
        // TODO
    }

    /**
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
            actionBean = (JsonBean) dagEngine.getWorkflowAction(actionId);
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
            jobBean = (JsonBean) coordJob;
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
            actionBean = (JsonBean) coordEngine.getCoordAction(actionId);
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
