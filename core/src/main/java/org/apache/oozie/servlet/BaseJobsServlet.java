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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.json.simple.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;

public abstract class BaseJobsServlet extends JsonRestServlet {

    private static final JsonRestServlet.ResourceInfo RESOURCES_INFO[] = new JsonRestServlet.ResourceInfo[1];

    static {
        RESOURCES_INFO[0] = new JsonRestServlet.ResourceInfo("", Arrays.asList(
                "POST", "GET"), Arrays.asList(
                new JsonRestServlet.ParameterInfo(RestConstants.ACTION_PARAM,
                                                  String.class, false, Arrays.asList("POST")),
                new JsonRestServlet.ParameterInfo(
                        RestConstants.JOBS_FILTER_PARAM, String.class, false,
                        Arrays.asList("GET")),
                new JsonRestServlet.ParameterInfo(RestConstants.JOBTYPE_PARAM,
                                                  String.class, false, Arrays.asList("GET")),
                new JsonRestServlet.ParameterInfo(RestConstants.OFFSET_PARAM,
                                                  String.class, false, Arrays.asList("GET")),
                new JsonRestServlet.ParameterInfo(RestConstants.LEN_PARAM,
                                                  String.class, false, Arrays.asList("GET")),

                new JsonRestServlet.ParameterInfo(
                        RestConstants.JOBS_EXTERNAL_ID_PARAM, String.class,
                        false, Arrays.asList("GET"))));
    }

    public BaseJobsServlet(String instrumentationName) {
        super(instrumentationName, RESOURCES_INFO);
    }

    /**
     * Create a job.
     */
    @SuppressWarnings("unchecked")
    protected void doPost(HttpServletRequest request,
                          HttpServletResponse response) throws ServletException, IOException {
        String authTok = getAuthToken(request);
        /*
       * Enumeration p = request.getAttributeNames();
       * for(;p.hasMoreElements();){ String key = (String)p.nextElement();
       * XLog.getLog(getClass()).warn(" key "+ key + " val "+ (String)
       * request.getAttribute(key)); }
       */
        validateContentType(request, RestConstants.XML_CONTENT_TYPE);

        request.setAttribute(AUDIT_OPERATION, request
                .getParameter(RestConstants.ACTION_PARAM));

        Configuration conf = new XConfiguration(request.getInputStream());

        stopCron();
        conf = XConfiguration.trim(conf);
        validateJobConfiguration(conf);
        BaseJobServlet.checkAuthorizationForApp(getUser(request), conf);

        JSONObject json = submitJob(request, conf);
        startCron();
        sendJsonResponse(response, HttpServletResponse.SC_CREATED, json);

    }

    /**
     * Return information about jobs.
     */
    @SuppressWarnings("unchecked")
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String externalId = request
                .getParameter(RestConstants.JOBS_EXTERNAL_ID_PARAM);
        if (externalId != null) {
            stopCron();
            JSONObject json = getJobIdForExternalId(request, externalId);
            startCron();
            sendJsonResponse(response, HttpServletResponse.SC_OK, json);
        }
        else {
            stopCron();
            // Configuration conf = new
            // XConfiguration(request.getInputStream());
            JSONObject json = getJobs(request);
            startCron();
            sendJsonResponse(response, HttpServletResponse.SC_OK, json);
        }
    }

    /**
     * abstract method to submit a job, either workflow or coordinator in the case of workflow job, there is an optional
     * flag in request to indicate if want this job to be started immediately or not
     *
     * @param request
     * @param conf
     * @return
     * @throws XServletException
     * @throws IOException TODO
     */
    abstract JSONObject submitJob(HttpServletRequest request, Configuration conf)
            throws XServletException, IOException;

    /**
     * abstract method to get a job from external ID
     *
     * @param request
     * @param externalId
     * @return JSONObject for the requested job
     * @throws XServletException
     * @throws IOException TODO
     */
    abstract JSONObject getJobIdForExternalId(HttpServletRequest request,
                                              String externalId) throws XServletException, IOException;

    /**
     * abstract method to get a list of workflow jobs
     *
     * @param request
     * @param conf
     * @return
     * @throws XServletException
     * @throws IOException TODO
     */
    abstract JSONObject getJobs(HttpServletRequest request)
            throws XServletException, IOException;

    static void validateJobConfiguration(Configuration conf) throws XServletException {
        if (conf.get(OozieClient.USER_NAME) == null) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0401,
                                        OozieClient.USER_NAME);
        }

        //TODO: it should use KerberosHadoopAccessorService.KERBEROS_AUTH_ENABLED once 20.1 is not used anymore
        if (Services.get().getConf().getBoolean("oozie.service.HadoopAccessorService.kerberos.enabled", false)) {
            if (conf.get(WorkflowAppService.HADOOP_JT_KERBEROS_NAME) == null) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0401,
                                            WorkflowAppService.HADOOP_JT_KERBEROS_NAME);
            }
            if (conf.get(WorkflowAppService.HADOOP_NN_KERBEROS_NAME) == null) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0401,
                                            WorkflowAppService.HADOOP_NN_KERBEROS_NAME);
            }
        }
        else {
            conf.set(WorkflowAppService.HADOOP_JT_KERBEROS_NAME, "");
            conf.set(WorkflowAppService.HADOOP_NN_KERBEROS_NAME, "");
        }
    }
}
