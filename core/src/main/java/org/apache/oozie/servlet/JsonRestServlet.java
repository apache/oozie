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

import org.apache.oozie.client.OozieClient.SYSTEM_MODE;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.ProxyUserService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.ErrorCode;
import org.json.simple.JSONObject;
import org.json.simple.JSONStreamAware;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class for Oozie web service API Servlets. <p/> This class provides common instrumentation, error logging and
 * other common functionality.
 */
public abstract class JsonRestServlet extends HttpServlet {

    static final String JSON_UTF8 = RestConstants.JSON_CONTENT_TYPE + "; charset=\"UTF-8\"";

    protected static final String XML_UTF8 = RestConstants.XML_CONTENT_TYPE + "; charset=\"UTF-8\"";

    protected static final String TEXT_UTF8 = RestConstants.TEXT_CONTENT_TYPE + "; charset=\"UTF-8\"";

    protected static final String AUDIT_OPERATION = "audit.operation";
    protected static final String AUDIT_PARAM = "audit.param";
    protected static final String AUDIT_ERROR_CODE = "audit.error.code";
    protected static final String AUDIT_ERROR_MESSAGE = "audit.error.message";
    protected static final String AUDIT_HTTP_STATUS_CODE = "audit.http.status.code";

    private XLog auditLog;
    XLog.Info logInfo;

    /**
     * This bean defines a query string parameter.
     */
    public static class ParameterInfo {
        private String name;
        private Class type;
        private List<String> methods;
        private boolean required;

        /**
         * Creates a ParameterInfo with querystring parameter definition.
         *
         * @param name querystring parameter name.
         * @param type type for the parameter value, valid types are: <code>Integer, Boolean and String</code>
         * @param required indicates if the parameter is required.
         * @param methods HTTP methods the parameter is used by.
         */
        public ParameterInfo(String name, Class type, boolean required, List<String> methods) {
            this.name = ParamChecker.notEmpty(name, "name");
            if (type != Integer.class && type != Boolean.class && type != String.class) {
                throw new IllegalArgumentException("Type must be integer, boolean or string");
            }
            this.type = ParamChecker.notNull(type, "type");
            this.required = required;
            this.methods = ParamChecker.notNull(methods, "methods");
        }

    }

    /**
     * This bean defines a REST resource.
     */
    public static class ResourceInfo {
        private String name;
        private boolean wildcard;
        private List<String> methods;
        private Map<String, ParameterInfo> parameters = new HashMap<String, ParameterInfo>();

        /**
         * Creates a ResourceInfo with a REST resource definition.
         *
         * @param name name of the REST resource, it can be an fixed resource name, empty or a wildcard ('*').
         * @param methods HTTP methods supported by the resource.
         * @param parameters parameters supported by the resource.
         */
        public ResourceInfo(String name, List<String> methods, List<ParameterInfo> parameters) {
            this.name = name;
            wildcard = name.equals("*");
            for (ParameterInfo parameter : parameters) {
                this.parameters.put(parameter.name, parameter);
            }
            this.methods = ParamChecker.notNull(methods, "methods");
        }
    }

    /**
     * Name of the instrumentation group for the WS layer, value is 'webservices'.
     */
    protected static final String INSTRUMENTATION_GROUP = "webservices";

    private static final String INSTR_TOTAL_REQUESTS_SAMPLER = "requests";
    private static final String INSTR_TOTAL_REQUESTS_COUNTER = "requests";
    private static final String INSTR_TOTAL_FAILED_REQUESTS_COUNTER = "failed";
    private static AtomicLong TOTAL_REQUESTS_SAMPLER_COUNTER;

    private Instrumentation instrumentation;
    private String instrumentationName;
    private AtomicLong samplerCounter = new AtomicLong();
    private ThreadLocal<Instrumentation.Cron> requestCron = new ThreadLocal<Instrumentation.Cron>();
    private List<ResourceInfo> resourcesInfo = new ArrayList<ResourceInfo>();
    private boolean allowSafeModeChanges;

    /**
     * Creates a servlet with a specified instrumentation sampler name for its requests.
     *
     * @param instrumentationName instrumentation name for timer and samplers for the servlet.
     * @param resourcesInfo list of resource definitions supported by the servlet, empty and wildcard resources must be
     * the last ones, in that order, first empty and the wildcard.
     */
    public JsonRestServlet(String instrumentationName, ResourceInfo... resourcesInfo) {
        this.instrumentationName = ParamChecker.notEmpty(instrumentationName, "instrumentationName");
        if (resourcesInfo.length == 0) {
            throw new IllegalArgumentException("There must be at least one ResourceInfo");
        }
        this.resourcesInfo = Arrays.asList(resourcesInfo);
        auditLog = XLog.getLog("oozieaudit");
        auditLog.setMsgPrefix("");
        logInfo = new XLog.Info(XLog.Info.get());
    }

    /**
     * Enable HTTP POST/PUT/DELETE methods while in safe mode.
     *
     * @param allow <code>true</code> enabled safe mode changes, <code>false</code> disable safe mode changes
     * (default).
     */
    protected void setAllowSafeModeChanges(boolean allow) {
        allowSafeModeChanges = allow;
    }

    /**
     * Define an instrumentation sampler. <p/> Sampling period is 60 seconds, the sampling frequency is 1 second. <p/>
     * The instrumentation group used is {@link #INSTRUMENTATION_GROUP}.
     *
     * @param samplerName sampler name.
     * @param samplerCounter sampler counter.
     */
    private void defineSampler(String samplerName, final AtomicLong samplerCounter) {
        instrumentation.addSampler(INSTRUMENTATION_GROUP, samplerName, 60, 1, new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return samplerCounter.get();
            }
        });
    }

    /**
     * Add an instrumentation cron.
     *
     * @param name name of the timer for the cron.
     * @param cron cron to add to a instrumentation timer.
     */
    private void addCron(String name, Instrumentation.Cron cron) {
        instrumentation.addCron(INSTRUMENTATION_GROUP, name, cron);
    }

    /**
     * Start the request instrumentation cron.
     */
    protected void startCron() {
        requestCron.get().start();
    }

    /**
     * Stop the request instrumentation cron.
     */
    protected void stopCron() {
        requestCron.get().stop();
    }

    /**
     * Initializes total request and servlet request samplers.
     */
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);
        instrumentation = Services.get().get(InstrumentationService.class).get();
        synchronized (JsonRestServlet.class) {
            if (TOTAL_REQUESTS_SAMPLER_COUNTER == null) {
                TOTAL_REQUESTS_SAMPLER_COUNTER = new AtomicLong();
                defineSampler(INSTR_TOTAL_REQUESTS_SAMPLER, TOTAL_REQUESTS_SAMPLER_COUNTER);
            }
        }
        defineSampler(instrumentationName, samplerCounter);
    }

    /**
     * Convenience method for instrumentation counters.
     *
     * @param name counter name.
     * @param count count to increment the counter.
     */
    private void incrCounter(String name, int count) {
        if (instrumentation != null) {
            instrumentation.incr(INSTRUMENTATION_GROUP, name, count);
        }
    }

    /**
     * Logs audit information for write requests to the audit log.
     *
     * @param request the http request.
     */
    private void logAuditInfo(HttpServletRequest request) {
        if (request.getAttribute(AUDIT_OPERATION) != null) {
            Integer httpStatusCode = (Integer) request.getAttribute(AUDIT_HTTP_STATUS_CODE);
            httpStatusCode = (httpStatusCode != null) ? httpStatusCode : HttpServletResponse.SC_OK;
            String status = (httpStatusCode == HttpServletResponse.SC_OK) ? "SUCCESS" : "FAILED";
            String operation = (String) request.getAttribute(AUDIT_OPERATION);
            String param = (String) request.getAttribute(AUDIT_PARAM);
            String user = XLog.Info.get().getParameter(XLogService.USER);
            String group = XLog.Info.get().getParameter(XLogService.GROUP);
            String jobId = XLog.Info.get().getParameter(DagXLogInfoService.JOB);
            String app = XLog.Info.get().getParameter(DagXLogInfoService.APP);

            String errorCode = (String) request.getAttribute(AUDIT_ERROR_CODE);
            String errorMessage = (String) request.getAttribute(AUDIT_ERROR_MESSAGE);
            String hostDetail = request.getRemoteAddr();

            auditLog.info(
                    "IP [{0}], USER [{1}], GROUP [{2}], APP [{3}], JOBID [{4}], OPERATION [{5}], PARAMETER [{6}], STATUS [{7}],"
                            + " HTTPCODE [{8}], ERRORCODE [{9}], ERRORMESSAGE [{10}]", hostDetail, user, group, app,
                    jobId, operation, param, status, httpStatusCode, errorCode, errorMessage);
        }
    }

    /**
     * Dispatches to super after loginfo and intrumentation handling. In case of errors dispatches error response codes
     * and does error logging.
     */
    @SuppressWarnings("unchecked")
    protected final void service(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException {
        //if (Services.get().isSafeMode() && !request.getMethod().equals("GET") && !allowSafeModeChanges) {
        if (Services.get().getSystemMode() != SYSTEM_MODE.NORMAL && !request.getMethod().equals("GET") && !allowSafeModeChanges) {
            sendErrorResponse(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, ErrorCode.E0002.toString(),
                              ErrorCode.E0002.getTemplate());
            return;
        }
        Instrumentation.Cron cron = new Instrumentation.Cron();
        requestCron.set(cron);
        try {
            cron.start();
            validateRestUrl(request.getMethod(), getResourceName(request), request.getParameterMap());
            XLog.Info.get().clear();
            String user = getUser(request);
            TOTAL_REQUESTS_SAMPLER_COUNTER.incrementAndGet();
            samplerCounter.incrementAndGet();
            //If trace is enabled then display the request headers
            XLog log = XLog.getLog(getClass());
            if (log.isTraceEnabled()){
             logHeaderInfo(request);
            }
            super.service(request, response);
        }
        catch (XServletException ex) {
            XLog log = XLog.getLog(getClass());
            log.warn("URL[{0} {1}] error[{2}], {3}", request.getMethod(), getRequestUrl(request), ex.getErrorCode(), ex
                    .getMessage(), ex);
            request.setAttribute(AUDIT_ERROR_MESSAGE, ex.getMessage());
            request.setAttribute(AUDIT_ERROR_CODE, ex.getErrorCode().toString());
            request.setAttribute(AUDIT_HTTP_STATUS_CODE, ex.getHttpStatusCode());
            incrCounter(INSTR_TOTAL_FAILED_REQUESTS_COUNTER, 1);
            sendErrorResponse(response, ex.getHttpStatusCode(), ex.getErrorCode().toString(), ex.getMessage());
        }
        catch (AccessControlException ex) {
            XLog log = XLog.getLog(getClass());
            log.error("URL[{0} {1}] error, {2}", request.getMethod(), getRequestUrl(request), ex.getMessage(), ex);
            incrCounter(INSTR_TOTAL_FAILED_REQUESTS_COUNTER, 1);
            sendErrorResponse(response, HttpServletResponse.SC_UNAUTHORIZED, ErrorCode.E1400.toString(),
                              ex.getMessage());
        }
        catch (IllegalArgumentException ex){
          XLog log = XLog.getLog(getClass());
          log.warn("URL[{0} {1}] user error, {2}", request.getMethod(), getRequestUrl(request), ex.getMessage(), ex);
          incrCounter(INSTR_TOTAL_FAILED_REQUESTS_COUNTER, 1);
          sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E1603.toString(),
                            ex.getMessage());
        }
        catch (RuntimeException ex) {
            XLog log = XLog.getLog(getClass());
            log.error("URL[{0} {1}] error, {2}", request.getMethod(), getRequestUrl(request), ex.getMessage(), ex);
            incrCounter(INSTR_TOTAL_FAILED_REQUESTS_COUNTER, 1);
            throw ex;
        }
        finally {
            logAuditInfo(request);
            TOTAL_REQUESTS_SAMPLER_COUNTER.decrementAndGet();
            incrCounter(INSTR_TOTAL_REQUESTS_COUNTER, 1);
            samplerCounter.decrementAndGet();
            XLog.Info.remove();
            cron.stop();
            // TODO
            incrCounter(instrumentationName, 1);
            incrCounter(instrumentationName + "-" + request.getMethod(), 1);
            addCron(instrumentationName, cron);
            addCron(instrumentationName + "-" + request.getMethod(), cron);
            requestCron.remove();
        }
    }

    private void logHeaderInfo(HttpServletRequest request){
        XLog log = XLog.getLog(getClass());
        StringBuilder traceInfo = new StringBuilder(4096);
            //Display request URL and request.getHeaderNames();
            Enumeration<String> names = (Enumeration<String>) request.getHeaderNames();
            traceInfo.append("Request URL: ").append(getRequestUrl(request)).append("\nRequest Headers:\n");
            while (names.hasMoreElements()) {
                String name = names.nextElement();
                String value = request.getHeader(name);
                traceInfo.append(name).append(" : ").append(value).append("\n");
            }
            log.trace(traceInfo);
    }

    private String getRequestUrl(HttpServletRequest request) {
        StringBuffer url = request.getRequestURL();
        if (request.getQueryString() != null) {
            url.append("?").append(request.getQueryString());
        }
        return url.toString();
    }

    /**
     * Sends a JSON response.
     *
     * @param response servlet response.
     * @param statusCode HTTP status code.
     * @param bean bean to send as JSON response.
     * @param timeZoneId time zone to use for dates in the JSON response.
     * @throws java.io.IOException thrown if the bean could not be serialized to the response output stream.
     */
    protected void sendJsonResponse(HttpServletResponse response, int statusCode, JsonBean bean, String timeZoneId) 
            throws IOException {
        response.setStatus(statusCode);
        JSONObject json = bean.toJSONObject(timeZoneId);
        response.setContentType(JSON_UTF8);
        json.writeJSONString(response.getWriter());
    }

    /**
     * Sends a error response.
     *
     * @param response servlet response.
     * @param statusCode HTTP status code.
     * @param error error code.
     * @param message error message.
     * @throws java.io.IOException thrown if the error response could not be set.
     */
    protected void sendErrorResponse(HttpServletResponse response, int statusCode, String error, String message)
            throws IOException {
        response.setHeader(RestConstants.OOZIE_ERROR_CODE, error);
        response.setHeader(RestConstants.OOZIE_ERROR_MESSAGE, message);
        response.sendError(statusCode);
    }

    protected void sendJsonResponse(HttpServletResponse response, int statusCode, JSONStreamAware json)
            throws IOException {
        if (statusCode == HttpServletResponse.SC_OK || statusCode == HttpServletResponse.SC_CREATED) {
            response.setStatus(statusCode);
        }
        else {
            response.sendError(statusCode);
        }
        response.setStatus(statusCode);
        response.setContentType(JSON_UTF8);
        json.writeJSONString(response.getWriter());
    }

    /**
     * Validates REST URL using the ResourceInfos of the servlet.
     *
     * @param method HTTP method.
     * @param resourceName resource name.
     * @param queryStringParams query string parameters.
     * @throws javax.servlet.ServletException thrown if the resource name or parameters are incorrect.
     */
    @SuppressWarnings("unchecked")
    protected void validateRestUrl(String method, String resourceName, Map<String, String[]> queryStringParams)
            throws ServletException {

        if (resourceName.contains("/")) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0301, resourceName);
        }

        boolean valid = false;
        for (int i = 0; !valid && i < resourcesInfo.size(); i++) {
            ResourceInfo resourceInfo = resourcesInfo.get(i);
            if (resourceInfo.name.equals(resourceName) || resourceInfo.wildcard) {
                if (!resourceInfo.methods.contains(method)) {
                    throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0301, resourceName);
                }
                for (Map.Entry<String, String[]> entry : queryStringParams.entrySet()) {
                    String name = entry.getKey();
                    ParameterInfo parameterInfo = resourceInfo.parameters.get(name);
                    if (parameterInfo != null) {
                        if (!parameterInfo.methods.contains(method)) {
                            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, name);
                        }
                        String value = entry.getValue()[0].trim();
                        if (parameterInfo.type.equals(Boolean.class)) {
                            value = value.toLowerCase();
                            if (!value.equals("true") && !value.equals("false")) {
                                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0304, name,
                                                            "boolean");
                            }
                        }
                        if (parameterInfo.type.equals(Integer.class)) {
                            try {
                                Integer.parseInt(value);
                            }
                            catch (NumberFormatException ex) {
                                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0304, name,
                                                            "integer");
                            }
                        }
                    }
                }
                for (ParameterInfo parameterInfo : resourceInfo.parameters.values()) {
                    if (parameterInfo.methods.contains(method) && parameterInfo.required
                            && queryStringParams.get(parameterInfo.name) == null) {
                        throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0305,
                                                    parameterInfo.name);
                    }
                }
                valid = true;
            }
        }
        if (!valid) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0301, resourceName);
        }
    }

    /**
     * Return the resource name of the request. <p/> The resource name is the whole extra path. If the extra path starts
     * with '/', the first '/' is trimmed.
     *
     * @param request request instance
     * @return the resource name, <code>null</code> if none.
     */
    protected String getResourceName(HttpServletRequest request) {
        String requestPath = request.getPathInfo();
        if (requestPath != null) {
            while (requestPath.startsWith("/")) {
                requestPath = requestPath.substring(1);
            }
            requestPath = requestPath.trim();
        }
        else {
            requestPath = "";
        }
        return requestPath;
    }

    /**
     * Return the request content type, lowercase and without attributes.
     *
     * @param request servlet request.
     * @return the request content type, <code>null</code> if none.
     */
    protected String getContentType(HttpServletRequest request) {
        String contentType = request.getContentType();
        if (contentType != null) {
            int index = contentType.indexOf(";");
            if (index > -1) {
                contentType = contentType.substring(0, index);
            }
            contentType = contentType.toLowerCase();
        }
        return contentType;
    }

    /**
     * Validate and return the content type of the request.
     *
     * @param request servlet request.
     * @param expected expected contentType.
     * @return the normalized content type (lowercase and without modifiers).
     * @throws XServletException thrown if the content type is invalid.
     */
    protected String validateContentType(HttpServletRequest request, String expected) throws XServletException {
        String contentType = getContentType(request);
        if (contentType == null || contentType.trim().length() == 0) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0300, contentType);
        }
        if (!contentType.equals(expected)) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0300, contentType);
        }
        return contentType;
    }

    /**
     * Request attribute constant for the authenticatio token.
     */
    public static final String AUTH_TOKEN = "oozie.auth.token";

    /**
     * Request attribute constant for the user name.
     */
    public static final String USER_NAME = "oozie.user.name";

    protected static final String UNDEF = "?";

    /**
     * Return the user name of the request if any.
     *
     * @param request request.
     * @return the user name, <code>null</code> if there is none.
     */
    protected String getUser(HttpServletRequest request) {
        String userName = (String) request.getAttribute(USER_NAME);

        String doAsUserName = request.getParameter(RestConstants.DO_AS_PARAM);
        if (doAsUserName != null && !doAsUserName.equals(userName)) {
            ProxyUserService proxyUser = Services.get().get(ProxyUserService.class);
            try {
                proxyUser.validate(userName, HostnameFilter.get(), doAsUserName);
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            auditLog.info("Proxy user [{0}] DoAs user [{1}] Request [{2}]", userName, doAsUserName,
                          getRequestUrl(request));

            XLog.Info.get().setParameter(XLogService.USER, userName + " doAs " + doAsUserName);

            userName = doAsUserName;
        }
        else {
            XLog.Info.get().setParameter(XLogService.USER, userName);
        }
        return (userName != null) ? userName : UNDEF;
    }

    /**
     * Set the thread local log info with the given information.
     *
     * @param actionid action ID.
     */
    protected void setLogInfo(String actionid) {
        LogUtils.setLogInfo(actionid);
    }
}
