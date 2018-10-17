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

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BaseEngineException;
import org.apache.oozie.BulkResponseInfo;
import org.apache.oozie.BundleEngine;
import org.apache.oozie.BundleEngineException;
import org.apache.oozie.BundleJobInfo;
import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.CoordinatorJobInfo;
import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.OozieJsonFactory;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.cli.OozieCLI;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.client.rest.BulkResponseImpl;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.BundleEngineService;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class V1JobsServlet extends BaseJobsServlet {
    private static final XLog LOG = XLog.getLog(V1JobsServlet.class);

    private static final String INSTRUMENTATION_NAME = "v1jobs";
    private static final Set<String> httpJobType = new HashSet<String>(){{
        this.add(OozieCLI.HIVE_CMD);
        this.add(OozieCLI.SQOOP_CMD);
        this.add(OozieCLI.PIG_CMD);
        this.add(OozieCLI.MR_CMD);
    }};

    public V1JobsServlet() {
        super(INSTRUMENTATION_NAME);
    }

    /**
     * v1 service implementation to submit a job, either workflow or coordinator
     */
    @Override
    protected JSONObject submitJob(HttpServletRequest request, Configuration conf) throws XServletException,
            IOException {
        JSONObject json = null;

        String jobType = request.getParameter(RestConstants.JOBTYPE_PARAM);

        if (!getUser(request).equals(UNDEF)) {
            ConfigUtils.checkAndSetDisallowedProperties(conf,
                    getUser(request),
                    new XServletException(HttpServletResponse.SC_BAD_REQUEST,
                            ErrorCode.E0303,
                            "configuration",
                            OozieClient.USER_NAME),
                    false);
        }

        if (jobType == null) {
            String wfPath = conf.get(OozieClient.APP_PATH);
            String coordPath = conf.get(OozieClient.COORDINATOR_APP_PATH);
            String bundlePath = conf.get(OozieClient.BUNDLE_APP_PATH);

            ServletUtilities.validateAppPath(wfPath, coordPath, bundlePath);

            if (wfPath != null) {
                json = submitWorkflowJob(request, conf);
            }
            else if (coordPath != null) {
                json = submitCoordinatorJob(request, conf);
            }
            else {
                json = submitBundleJob(request, conf);
            }
        }
        else { // This is a http submission job
            if (httpJobType.contains(jobType)) {
                json = submitHttpJob(request, conf, jobType);
            }
            else {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                        RestConstants.JOBTYPE_PARAM, jobType);
            }
        }
        return json;
    }

    @Override
    protected void checkAndWriteApplicationXMLToHDFS(final String userName, final Configuration conf) throws XServletException {
        if (!Strings.isNullOrEmpty(conf.get(XOozieClient.IS_PROXY_SUBMISSION))
                && Boolean.valueOf(conf.get(XOozieClient.IS_PROXY_SUBMISSION))) {
            LOG.debug("Proxy submission in progress, no need to write application XML.");
            return;
        }

        final List<String> appPathsWithFileNames;
        if (!findAppPathsWithFileNames(conf.get(OozieClient.APP_PATH), "workflow.xml").isEmpty()) {
            appPathsWithFileNames = findAppPathsWithFileNames(conf.get(OozieClient.APP_PATH), "workflow.xml");
        }
        else if (!findAppPathsWithFileNames(conf.get(OozieClient.LIBPATH), "workflow.xml").isEmpty()) {
            appPathsWithFileNames = findAppPathsWithFileNames(conf.get(OozieClient.LIBPATH), "workflow.xml");
        }
        else if (!findAppPathsWithFileNames(conf.get(OozieClient.COORDINATOR_APP_PATH), "coordinator.xml").isEmpty()) {
            appPathsWithFileNames = findAppPathsWithFileNames(conf.get(OozieClient.COORDINATOR_APP_PATH), "coordinator.xml");
        }
        else {
            appPathsWithFileNames = findAppPathsWithFileNames(conf.get(OozieClient.BUNDLE_APP_PATH), "bundle.xml");
        }

        LOG.debug("Checking whether XML exists on HDFS. [appPathsWithFileNames={0}]", appPathsWithFileNames);

        for (final String appPathWithFileName : appPathsWithFileNames) {
            if (existsOnDFS(userName, appPathWithFileName)) {
                return;
            }
        }

        for (final String appPathWithFileName : appPathsWithFileNames) {
            final String sourceContent = conf.get(OozieClient.CONFIG_KEY_GENERATED_XML);
            if (sourceContent == null) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0307,
                        String.format("Configuration entry %s not present", OozieClient.CONFIG_KEY_GENERATED_XML));
            }

            if (tryCreateOnDFS(userName, appPathWithFileName, sourceContent)) {
                return;
            }
        }

        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0307,
                String.format("Could not create on HDFS any of the missing application XMLs [%s]",
                        appPathsWithFileNames));
    }

    private List<String> findAppPathsWithFileNames(final String appPaths, final String defaultFileName) {
        final List<String> appPathsWithFileNames = Lists.newArrayList();

        if (Strings.isNullOrEmpty(appPaths)) {
            return appPathsWithFileNames;
        }

        for (final String appPath : appPaths.split(",")) {
            if (appPath.endsWith(".xml")) {
                appPathsWithFileNames.add(appPath);
            }
            else {
                appPathsWithFileNames.add(appPath + File.separator + defaultFileName);
            }
        }

        return appPathsWithFileNames;
    }

    private boolean existsOnDFS(final String userName, final String appPathWithFileName) throws XServletException {
        try {
            final URI uri = new URI(appPathWithFileName);
            final HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            final Configuration fsConf = has.createConfiguration(uri.getAuthority());
            final FileSystem dfs = has.createFileSystem(userName, uri, fsConf);

            final Path path = new Path(uri.getPath());

            if (dfs.exists(path)) {
                if (!dfs.isFile(path)) {
                    final String errorMessage = String.format("HDFS path [%s] exists but is not a file.", path.toString());
                    LOG.error(errorMessage);
                    throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0307, errorMessage);
                }

                LOG.debug("HDFS path [{0}] is an existing file, no need to create.", path.toString());
                return true;
            }

            LOG.debug("HDFS path [{0}] is not an existing file.", path.toString());
            return false;
        }
        catch (final URISyntaxException | IOException | HadoopAccessorException e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0307,
                    String.format("Could not check whether file [%s] exists on HDFS. Error message: %s",
                            appPathWithFileName,  e.getMessage()));
        }
    }

    private boolean tryCreateOnDFS(final String userName, final String appPathWithFileName, final String sourceContent) {
        try {
            final URI uri = new URI(appPathWithFileName);
            final HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            final Configuration fsConf = has.createConfiguration(uri.getAuthority());
            final FileSystem dfs = has.createFileSystem(userName, uri, fsConf);

            final Path path = new Path(uri.getPath());

            LOG.debug("HDFS path [{0}] does not exist, will try to create.", path.toString());

            try (final FSDataOutputStream target = dfs.create(path)) {
                LOG.debug("HDFS path [{0}] created.", path.toString());

                IOUtils.copyCharStream(new StringReader(sourceContent), new OutputStreamWriter(target, StandardCharsets.UTF_8));
            }

            LOG.debug("XML written to HDFS file [{0}].", path.toString());

            return true;
        }
        catch (final URISyntaxException | IOException | HadoopAccessorException e) {
            LOG.warn("Could not write XML [%s] to HDFS. Error message: %s", appPathWithFileName, e.getMessage());
            return false;
        }
    }

    /**
     * v1 service implementation to get a JSONObject representation of a job from its external ID
     */
    @Override
    protected JSONObject getJobIdForExternalId(HttpServletRequest request, String externalId) throws XServletException,
            IOException {
        JSONObject json = null;
        /*
         * Configuration conf = new XConfiguration(); String wfPath =
         * conf.get(OozieClient.APP_PATH); String coordPath =
         * conf.get(OozieClient.COORDINATOR_APP_PATH);
         *
         * ServletUtilities.ValidateAppPath(wfPath, coordPath);
         */
        String jobtype = request.getParameter(RestConstants.JOBTYPE_PARAM);
        jobtype = (jobtype != null) ? jobtype : "wf";
        if (jobtype.contains("wf")) {
            json = getWorkflowJobIdForExternalId(request, externalId);
        }
        else {
            json = getCoordinatorJobIdForExternalId(request, externalId);
        }
        return json;
    }

    /**
     * v1 service implementation to get a list of workflows, coordinators, or bundles, with filtering or interested
     * windows embedded in the request object
     */
    @Override
    protected JSONObject getJobs(HttpServletRequest request) throws XServletException, IOException {
        JSONObject json = null;
        String isBulk = request.getParameter(RestConstants.JOBS_BULK_PARAM);
        if(isBulk != null) {
            json = getBulkJobs(request);
        } else {
            String jobtype = request.getParameter(RestConstants.JOBTYPE_PARAM);
            jobtype = (jobtype != null) ? jobtype : "wf";

            if (jobtype.contains("wf")) {
                json = getWorkflowJobs(request);
            }
            else if (jobtype.contains("coord")) {
                json = getCoordinatorJobs(request);
            }
            else if (jobtype.contains("bundle")) {
                json = getBundleJobs(request);
            }
        }
        return json;
    }

    /**
     * v1 service implementation to submit a workflow job
     */
    @SuppressWarnings("unchecked")
    private JSONObject submitWorkflowJob(HttpServletRequest request, Configuration conf) throws XServletException {

        JSONObject json = new JSONObject();

        try {
            String action = request.getParameter(RestConstants.ACTION_PARAM);
            if (action != null && !action.equals(RestConstants.JOB_ACTION_START)
                    && !action.equals(RestConstants.JOB_ACTION_DRYRUN)) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                        RestConstants.ACTION_PARAM, action);
            }
            boolean startJob = (action != null);
            String user = conf.get(OozieClient.USER_NAME);
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user);
            String id;
            boolean dryrun = false;
            if (action != null) {
                dryrun = (action.equals(RestConstants.JOB_ACTION_DRYRUN));
            }
            if (dryrun) {
                id = dagEngine.dryRunSubmit(conf);
            }
            else {
                id = dagEngine.submitJob(conf, startJob);
            }
            json.put(JsonTags.JOB_ID, id);
        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * v1 service implementation to submit a coordinator job
     */
    @SuppressWarnings("unchecked")
    private JSONObject submitCoordinatorJob(HttpServletRequest request, Configuration conf) throws XServletException {

        JSONObject json = new JSONObject();
        XLog.getLog(getClass()).warn("submitCoordinatorJob " + XmlUtils.prettyPrint(conf).toString());
        try {
            String action = request.getParameter(RestConstants.ACTION_PARAM);
            if (action != null && !action.equals(RestConstants.JOB_ACTION_START)
                    && !action.equals(RestConstants.JOB_ACTION_DRYRUN)) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                        RestConstants.ACTION_PARAM, action);
            }
            boolean startJob = (action != null);
            String user = conf.get(OozieClient.USER_NAME);
            CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                    user);
            String id = null;
            boolean dryrun = false;
            if (action != null) {
                dryrun = (action.equals(RestConstants.JOB_ACTION_DRYRUN));
            }
            if (dryrun) {
                id = coordEngine.dryRunSubmit(conf);
            }
            else {
                id = coordEngine.submitJob(conf, startJob);
            }
            json.put(JsonTags.JOB_ID, id);
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * v1 service implementation to submit a bundle job
     */
    @SuppressWarnings("unchecked")
    private JSONObject submitBundleJob(HttpServletRequest request, Configuration conf) throws XServletException {
        JSONObject json = new JSONObject();
        XLog.getLog(getClass()).warn("submitBundleJob " + XmlUtils.prettyPrint(conf).toString());
        try {
            String action = request.getParameter(RestConstants.ACTION_PARAM);
            if (action != null && !action.equals(RestConstants.JOB_ACTION_START)
                    && !action.equals(RestConstants.JOB_ACTION_DRYRUN)) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                        RestConstants.ACTION_PARAM, action);
            }
            boolean startJob = (action != null);
            String user = conf.get(OozieClient.USER_NAME);
            BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(user);
            String id = null;
            boolean dryrun = false;
            if (action != null) {
                dryrun = (action.equals(RestConstants.JOB_ACTION_DRYRUN));
            }
            if (dryrun) {
                id = bundleEngine.dryRunSubmit(conf);
            }
            else {
                id = bundleEngine.submitJob(conf, startJob);
            }
            json.put(JsonTags.JOB_ID, id);
        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * v1 service implementation to get a JSONObject representation of a job from its external ID
     */
    @SuppressWarnings("unchecked")
    private JSONObject getWorkflowJobIdForExternalId(HttpServletRequest request, String externalId)
            throws XServletException {
        JSONObject json = new JSONObject();
        try {
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));
            String jobId = dagEngine.getJobIdForExternalId(externalId);
            json.put(JsonTags.JOB_ID, jobId);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return json;
    }

    /**
     * v1 service implementation to get a JSONObject representation of a job from its external ID
     */
    private JSONObject getCoordinatorJobIdForExternalId(HttpServletRequest request, String externalId)
            throws XServletException {
        JSONObject json = new JSONObject();
        return json;
    }

    /**
     * v1 service implementation to get a list of workflows, with filtering or interested windows embedded in the
     * request object
     */
    private JSONObject getWorkflowJobs(HttpServletRequest request) throws XServletException {
        JSONObject json;
        try {
            String filter = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
            String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
            String lenStr = request.getParameter(RestConstants.LEN_PARAM);
            String timeZoneId = request.getParameter(RestConstants.TIME_ZONE_PARAM) == null
                    ? "GMT" : request.getParameter(RestConstants.TIME_ZONE_PARAM);
            int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
            start = (start < 1) ? 1 : start;
            int len = (lenStr != null) ? Integer.parseInt(lenStr) : 50;
            len = (len < 1) ? 50 : len;
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));
            WorkflowsInfo jobs = dagEngine.getJobs(filter, start, len);
            json = OozieJsonFactory.getWFJSONObject(jobs, timeZoneId);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * v1 service implementation to get a list of workflows, with filtering or interested windows embedded in the
     * request object
     */
    @SuppressWarnings("unchecked")
    private JSONObject getCoordinatorJobs(HttpServletRequest request) throws XServletException {
        JSONObject json;
        try {
            String filter = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
            String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
            String lenStr = request.getParameter(RestConstants.LEN_PARAM);
            String timeZoneId = request.getParameter(RestConstants.TIME_ZONE_PARAM) == null
                    ? "GMT" : request.getParameter(RestConstants.TIME_ZONE_PARAM);
            int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
            start = (start < 1) ? 1 : start;
            int len = (lenStr != null) ? Integer.parseInt(lenStr) : 50;
            len = (len < 1) ? 50 : len;
            CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(
                    getUser(request));
            CoordinatorJobInfo jobs = coordEngine.getCoordJobs(filter, start, len);
            json = OozieJsonFactory.getCoordJSONObject(jobs, timeZoneId);
        }
        catch (CoordinatorEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return json;
    }

    @SuppressWarnings("unchecked")
    private JSONObject getBundleJobs(HttpServletRequest request) throws XServletException {
        JSONObject json;
        try {
            String filter = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
            String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
            String lenStr = request.getParameter(RestConstants.LEN_PARAM);
            String timeZoneId = request.getParameter(RestConstants.TIME_ZONE_PARAM) == null
                    ? "GMT" : request.getParameter(RestConstants.TIME_ZONE_PARAM);
            int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
            start = (start < 1) ? 1 : start;
            int len = (lenStr != null) ? Integer.parseInt(lenStr) : 50;
            len = (len < 1) ? 50 : len;

            BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
            BundleJobInfo jobs = bundleEngine.getBundleJobs(filter, start, len);
            json = OozieJsonFactory.getBundleJSONObject(jobs, timeZoneId);
        }
        catch (BundleEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return json;
    }

    @SuppressWarnings("unchecked")
    private JSONObject getBulkJobs(HttpServletRequest request) throws XServletException, IOException {
        JSONObject json = new JSONObject();
        try {
            String bulkFilter = request.getParameter(RestConstants.JOBS_BULK_PARAM); //REST API
            String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
            String lenStr = request.getParameter(RestConstants.LEN_PARAM);
            String timeZoneId = request.getParameter(RestConstants.TIME_ZONE_PARAM) == null
                    ? "GMT" : request.getParameter(RestConstants.TIME_ZONE_PARAM);
            int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
            start = (start < 1) ? 1 : start;
            int len = (lenStr != null) ? Integer.parseInt(lenStr) : 50;
            len = (len < 1) ? 50 : len;

            BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
            BulkResponseInfo bulkResponse = bundleEngine.getBulkJobs(bulkFilter, start, len);
            List<BulkResponseImpl> responsesToJson = bulkResponse.getResponses();

            json.put(JsonTags.BULK_RESPONSES, BulkResponseImpl.toJSONArray(responsesToJson, timeZoneId));
            json.put(JsonTags.BULK_RESPONSE_TOTAL, bulkResponse.getTotal());
            json.put(JsonTags.BULK_RESPONSE_OFFSET, bulkResponse.getStart());
            json.put(JsonTags.BULK_RESPONSE_LEN, bulkResponse.getLen());

        }
        catch (BaseEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }
        return json;
    }

    /**
     * service implementation to submit a http job
     */
    private JSONObject submitHttpJob(HttpServletRequest request, Configuration conf, String jobType)
            throws XServletException {
        JSONObject json = new JSONObject();

        try {
            String user = conf.get(OozieClient.USER_NAME);
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user);
            String id = dagEngine.submitHttpJob(conf, jobType);
            json.put(JsonTags.JOB_ID, id);
        }
        catch (DagEngineException ex) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
        }

        return json;
    }

    /**
     * service implementation to bulk kill jobs
     * @param request
     * @param response
     * @return bulkModifyJobs implementation to bulk kill jobs
     * @throws XServletException
     * @throws IOException
     */
    @Override
    protected JSONObject killJobs(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        return bulkModifyJobs(request, response);
    }

    /**
     * service implementation to bulk suspend jobs
     * @param request
     * @param response
     * @return bulkModifyJobs implementation to bulk suspend jobs
     * @throws XServletException
     * @throws IOException
     */
    @Override
    protected JSONObject suspendJobs(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        return bulkModifyJobs(request, response);
    }

    /**
     * service implementation to bulk resume jobs
     * @param request
     * @param response
     * @return bulkModifyJobs implementation to bulk resume jobs
     * @throws XServletException
     * @throws IOException
     */
    @Override
    protected JSONObject resumeJobs(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        return bulkModifyJobs(request, response);
    }

    private JSONObject bulkModifyJobs(HttpServletRequest request, HttpServletResponse response) throws XServletException,
            IOException {
        String action = request.getParameter(RestConstants.ACTION_PARAM);
        String jobType = request.getParameter(RestConstants.JOBTYPE_PARAM);
        String filter = request.getParameter(RestConstants.JOBS_FILTER_PARAM);
        String startStr = request.getParameter(RestConstants.OFFSET_PARAM);
        String lenStr = request.getParameter(RestConstants.LEN_PARAM);
        String timeZoneId = request.getParameter(RestConstants.TIME_ZONE_PARAM) == null
                ? "GMT" : request.getParameter(RestConstants.TIME_ZONE_PARAM);

        int start = (startStr != null) ? Integer.parseInt(startStr) : 1;
        start = (start < 1) ? 1 : start;
        int len = (lenStr != null) ? Integer.parseInt(lenStr) : 50;
        len = (len < 1) ? 50 : len;

        JSONObject json;
        List<String> ids = new ArrayList<String>();

        if (jobType.equals("wf")) {
            WorkflowsInfo jobs;
            DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));
            try {
                switch (action) {
                    case RestConstants.JOB_ACTION_KILL:
                        jobs = dagEngine.killJobs(filter, start, len);
                        break;
                    case RestConstants.JOB_ACTION_SUSPEND:
                        jobs = dagEngine.suspendJobs(filter, start, len);
                        break;
                    case RestConstants.JOB_ACTION_RESUME:
                        jobs = dagEngine.resumeJobs(filter, start, len);
                        break;
                    default:
                        throw new DagEngineException(ErrorCode.E0301, action);
                }
            } catch (DagEngineException ex) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
            }
            json = OozieJsonFactory.getWFJSONObject(jobs, timeZoneId);
        }
        else if (jobType.equals("bundle")) {
            BundleJobInfo jobs;
            BundleEngine bundleEngine = Services.get().get(BundleEngineService.class).getBundleEngine(getUser(request));
            try {
                switch (action) {
                    case RestConstants.JOB_ACTION_KILL:
                        jobs = bundleEngine.killJobs(filter, start, len);
                        break;
                    case RestConstants.JOB_ACTION_SUSPEND:
                        jobs = bundleEngine.suspendJobs(filter, start, len);
                        break;
                    case RestConstants.JOB_ACTION_RESUME:
                        jobs = bundleEngine.resumeJobs(filter, start, len);
                        break;
                    default:
                        throw new BundleEngineException(ErrorCode.E0301, action);
                }
            } catch (BundleEngineException ex) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
            }
            json = OozieJsonFactory.getBundleJSONObject(jobs, timeZoneId);
        }
        else {
            CoordinatorJobInfo jobs;
            CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).
                    getCoordinatorEngine(getUser(request));
            try {
                switch (action) {
                    case RestConstants.JOB_ACTION_KILL:
                        jobs = coordEngine.killJobs(filter, start, len);
                        break;
                    case RestConstants.JOB_ACTION_SUSPEND:
                        jobs = coordEngine.suspendJobs(filter, start, len);
                        break;
                    case RestConstants.JOB_ACTION_RESUME:
                        jobs = coordEngine.resumeJobs(filter, start, len);
                        break;
                    default:
                        throw new CoordinatorEngineException(ErrorCode.E0301, action);
                }
            } catch (CoordinatorEngineException ex) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ex);
            }
            json = OozieJsonFactory.getCoordJSONObject(jobs, timeZoneId);
        }
        json.put(JsonTags.JOB_IDS, toJSONArray(ids));
        return json;
    }

    private static JSONArray toJSONArray(List<String> ids) {
        JSONArray array = new JSONArray();
        for (String id : ids) {
            array.add(id);
        }
        return array;
    }
}
