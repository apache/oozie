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
package org.apache.oozie.client;

import org.apache.oozie.client.rest.JsonWorkflowJob;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.BuildInfo;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Enumeration;
import java.util.concurrent.Callable;

/**
 * Client API to submit and manage Oozie workflow jobs against an Oozie intance.
 * <p/>
 * This class is thread safe.
 * <p/>
 * Syntax for filter for the {@link #getJobsInfo(String)}  {@link #getJobsInfo(String, int, int)}  methods:
 * <code>[NAME=VALUE][;NAME=VALUE]*</code>.
 * <p/>
 * Valid filter names are:
 * <p/>
 * <ul/>
 * <li>name: the workflow application name from the workflow definition.</li>
 * <li>user: the user that submitted the job.</li>
 * <li>group: the group for the job.</li>
 * <li>status: the status of the job.</li>
 * </ul>
 * <p/>
 * The query will do an AND among all the filter names.
 * The query will do an OR among all the filter values for the same name. Multiple values must be specified as
 * different name value pairs.
 */
public class OozieClient {

    public static final long WS_PROTOCOL_VERSION = 0;


    public static final String USER_NAME = "user.name";

    public static final String GROUP_NAME = "group.name";

    public static final String APP_PATH = "oozie.wf.application.path";

    public static final String EXTERNAL_ID = "oozie.wf.external.id";

    public static final String WORKFLOW_NOTIFICATION_URL = "oozie.wf.workflow.notification.url";

    public static final String ACTION_NOTIFICATION_URL = "oozie.wf.action.notification.url";

    public static final String RERUN_SKIP_NODES = "oozie.wf.rerun.skip.nodes";

    public static final String LOG_TOKEN = "oozie.wf.log.token";

    public static final String ACTION_MAX_RETRIES = "oozie.wf.action.max.retries";

    public static final String ACTION_RETRY_INTERVAL = "oozie.wf.action.retry.interval";


    public static final String FILTER_USER = "user";

    public static final String FILTER_GROUP = "group";

    public static final String FILTER_NAME = "name";

    public static final String FILTER_STATUS = "status";



    private String baseUrl;
    private String protocolUrl;
    private boolean validatedVersion = false;
    private Map<String, String> headers = new HashMap<String, String>();

    private static <T> T notNull(T obj, String name) {
        if (obj == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
        return obj;
    }

    private static String notEmpty(String str, String name) {
        if (str == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
        if (str.length() == 0) {
            throw new IllegalArgumentException(name + " cannot be empty");
        }
        return str;
    }

    protected OozieClient() {
    }

    /**
     * Create a Workflow client instance.
     *
     * @param oozieUrl URL of the Oozie instance it will interact with.
     */
    public OozieClient(String oozieUrl) {
        this.baseUrl = notEmpty(oozieUrl, "oozieUrl");
        if (!this.baseUrl.endsWith("/")) {
            this.baseUrl += "/";
        }
    }

    /**
     * Return the Oozie URL of the workflow client instance.
     * <p/>
     * This URL is the base URL fo the Oozie system, with not protocol versioning.
     *
     * @return the Oozie URL of the workflow client instance.
     */
    public String getOozieUrl() {
        return baseUrl;
    }

    /**
     * Return the Oozie URL used by the client and server for WS communications.
     * <p/>
     * This URL is the original URL plus the versioning element path.
     *
     * @return the Oozie URL used by the client and server for communication.
     * @throws OozieClientException thrown in the client and the server are not protocol compatible.
     */
    public String getProtocolUrl() throws OozieClientException {
        validateWSVersion();
        return protocolUrl;
    }

    /**
     * Validate that the Oozie client and server instances are protocol compatible.
     *
     * @throws OozieClientException thrown in the client and the server are not protocol compatible.
     */
    public synchronized void validateWSVersion() throws OozieClientException {
        if (!validatedVersion) {
            try {
                URL url = new URL(baseUrl + RestConstants.VERSIONS);
                HttpURLConnection conn = createConnection(url, "GET");
                if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    JSONArray array = (JSONArray) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                    if (!array.contains(WS_PROTOCOL_VERSION)) {
                        StringBuilder msg = new StringBuilder();
                        msg.append("Unsupported version [").append(WS_PROTOCOL_VERSION)
                                .append("], supported versions[");
                        String separator = "";
                        for (Object version : array) {
                            msg.append(separator).append(version);
                        }
                        msg.append("]");
                        throw new OozieClientException(OozieClientException.UNSUPPORTED_VERSION, msg.toString());
                    }
                }
                else {
                    handleError(conn);
                }
            }
            catch (IOException ex) {
                throw new OozieClientException(OozieClientException.IO_ERROR, ex);
            }
            protocolUrl =  baseUrl + "v" + WS_PROTOCOL_VERSION + "/";
            validatedVersion = true;
        }
    }

    /**
     * Create an empty configuration with just the {@link #USER_NAME} set to the JVM user name.
     *
     * @return an empty configuration.
     */
    public Properties createConfiguration() {
        Properties conf = new Properties();
        conf.setProperty(USER_NAME, System.getProperty("user.name"));
        return conf;
    }

    /**
     * Set a HTTP header to be used in the WS requests by the workflow instance.
     *
     * @param name header name.
     * @param value header value.
     */
    public void setHeader(String name, String value) {
        headers.put(notEmpty(name, "name"), notNull(value, "value"));
    }

    /**
     * Get the value of a set HTTP header from the workflow instance.
     *
     * @param name header name.
     * @return header value, <code>null</code> if not set.
     */
    public String getHeader(String name) {
        return headers.get(notEmpty(name, "name"));
    }

    /**
     * Remove a HTTP header from the workflow client instance.
     *
     * @param name header name.
     */
    public void removeHeader(String name) {
        headers.remove(notEmpty(name, "name"));
    }

    /**
     * Return an iterator with all the header names set in the workflow instance.
     *
     * @return header names.
     */
    public Iterator<String> getHeaderNames() {
        return Collections.unmodifiableMap(headers).keySet().iterator();
    }

    private URL createURL(String collection, String resource, Map<String, String> parameters)
            throws IOException, OozieClientException {
        validateWSVersion();
        StringBuilder sb = new StringBuilder();
        sb.append(protocolUrl).append(collection);
        if (resource != null && resource.length() > 0) {
            sb.append("/").append(resource);
        }
        if (parameters.size() > 0) {
            String separator = "?";
            for (Map.Entry<String, String> param : parameters.entrySet()) {
                if (param.getValue() != null) {
                    sb.append(separator).append(URLEncoder.encode(param.getKey(), "UTF-8")).append("=")
                            .append(URLEncoder.encode(param.getValue(), "UTF-8"));
                    separator = "&";
                }
            }
        }
        return new URL(sb.toString());
    }

    private HttpURLConnection createConnection(URL url, String method) throws IOException, OozieClientException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        if (method.equals("POST") || method.equals("PUT")) {
            conn.setDoOutput(true);
        }
        for (Map.Entry<String, String> header : headers.entrySet()) {
            conn.setRequestProperty(header.getKey(), header.getValue());
        }
        return conn;
    }

    private abstract class ClientCallable<T> implements Callable<T> {
        private String method;
        private String collection;
        private String resource;
        private Map<String, String> params;

        public ClientCallable(String method, String collection, String resource, Map<String, String> params) {
            this.method = method;
            this.collection = collection;
            this.resource = resource;
            this.params = params;
        }

        public T call() throws OozieClientException {
            try {
                URL url = createURL(collection, resource, params);
                HttpURLConnection conn = createConnection(url, method);
                return call(conn);
            }
            catch (IOException ex) {
                throw new OozieClientException(OozieClientException.IO_ERROR, ex);
            }

        }

        protected abstract T call(HttpURLConnection conn) throws IOException, OozieClientException;
    }

    private static void handleError(HttpURLConnection conn) throws IOException, OozieClientException {
        int status = conn.getResponseCode();
        String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
        String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);

        if(error == null) {
            error = "HTTP error code: " + status;
        }

        if(message == null){
            message = conn.getResponseMessage();
        }
        throw new OozieClientException(error, message);
    }

    private static Map<String, String> prepareParams(String... params) {
        Map<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < params.length; i = i + 2) {
            map.put(params[i], params[i + 1]);
        }
        return map;
    }

    public void writeToXml(Properties props, OutputStream out) throws IOException {
        try {
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            Element conf = doc.createElement("configuration");
            doc.appendChild(conf);
            conf.appendChild(doc.createTextNode("\n"));
            for (Enumeration e = props.keys(); e.hasMoreElements();) {
                String name = (String) e.nextElement();
                Object object = props.get(name);
                String value;
                if (object instanceof String) {
                    value = (String) object;
                }
                else {
                    continue;
                }
                Element propNode = doc.createElement("property");
                conf.appendChild(propNode);

                Element nameNode = doc.createElement("name");
                nameNode.appendChild(doc.createTextNode(name));
                propNode.appendChild(nameNode);

                Element valueNode = doc.createElement("value");
                valueNode.appendChild(doc.createTextNode(value));
                propNode.appendChild(valueNode);

                conf.appendChild(doc.createTextNode("\n"));
            }

            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(out);
            TransformerFactory transFactory = TransformerFactory.newInstance();
            Transformer transformer = transFactory.newTransformer();
            transformer.transform(source, result);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    private class JobSubmit extends ClientCallable<String> {
        private Properties conf;

        JobSubmit(Properties conf, boolean start) {
            super("POST", RestConstants.JOBS, "", (start) ? prepareParams(RestConstants.ACTION_PARAM,
                                                                          RestConstants.JOB_ACTION_START)
                                                          : prepareParams());
            this.conf = notNull(conf, "conf");
        }

        JobSubmit(String jobId, Properties conf) {
            super("PUT", RestConstants.JOB, notEmpty(jobId, "jobId"),
                  prepareParams(RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_RERUN));
            this.conf = notNull(conf, "conf");
        }

        protected String call(HttpURLConnection conn) throws IOException, OozieClientException {
            conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
            writeToXml(conf, conn.getOutputStream());
            if (conn.getResponseCode() == HttpURLConnection.HTTP_CREATED) {
                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
                return (String) json.get(JsonTags.JOB_ID);
            }
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                handleError(conn);
            }
            return null;
        }
    }

    /**
     * Submit a workflow job.
     *
     * @param conf job configuration.
     * @return the job Id.
     * @throws OozieClientException thrown if the job could not be submitted.
     */
    public String submit(Properties conf) throws OozieClientException {
        return (new JobSubmit(conf, false)).call();
    }

    private class JobAction extends ClientCallable<Void> {

        JobAction(String jobId, String action) {
            super("PUT", RestConstants.JOB, notEmpty(jobId, "jobId"),
                  prepareParams(RestConstants.ACTION_PARAM, action));
        }

        protected Void call(HttpURLConnection conn) throws IOException, OozieClientException {
            if (!(conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                handleError(conn);
            }
            return null;
        }
    }

    /**
     * Start a workflow job.
     *
     * @param jobId job Id.
     * @throws OozieClientException thrown if the job could not be started.
     */
    public void start(String jobId) throws OozieClientException {
        new JobAction(jobId, RestConstants.JOB_ACTION_START).call();
    }

    /**
     * Submit and start a workflow job.
     *
     * @param conf job configuration.
     * @return the job Id.
     * @throws OozieClientException thrown if the job could not be submitted.
     */
    public String run(Properties conf) throws OozieClientException {
        return (new JobSubmit(conf, true)).call();
    }

    /**
     * Rerun a workflow job.
     *
     * @param jobId job Id to rerun.
     * @param conf configuration information for the rerun.
     * @throws OozieClientException thrown if the job could not be started.
     */
    public void reRun(String jobId, Properties conf) throws OozieClientException {
        new JobSubmit(jobId, conf).call();
    }

    /**
     * Suspend a workflow job.
     *
     * @param jobId job Id.
     * @throws OozieClientException thrown if the job could not be suspended.
     */
    public void suspend(String jobId) throws OozieClientException {
        new JobAction(jobId, RestConstants.JOB_ACTION_SUSPEND).call();
    }

    /**
     * Resume a workflow job.
     *
     * @param jobId job Id.
     * @throws OozieClientException thrown if the job could not be resume.
     */
    public void resume(String jobId) throws OozieClientException {
        new JobAction(jobId, RestConstants.JOB_ACTION_RESUME).call();
    }

    /**
     * Kill a workflow job.
     *
     * @param jobId job Id.
     * @throws OozieClientException thrown if the job could not be killed.
     */
    public void kill(String jobId) throws OozieClientException {
        new JobAction(jobId, RestConstants.JOB_ACTION_KILL).call();
    }

    private class JobInfo extends ClientCallable<WorkflowJob> {

        JobInfo(String jobId) {
            super("GET", RestConstants.JOB, notEmpty(jobId, "jobId"),
                  prepareParams(RestConstants.JOB_SHOW_PARAM, RestConstants.JOB_SHOW_INFO));
        }

        protected WorkflowJob call(HttpURLConnection conn) throws IOException, OozieClientException {
            if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                Reader reader = new InputStreamReader(conn.getInputStream());
                JSONObject json = (JSONObject) JSONValue.parse(reader);
                return new JsonWorkflowJob(json);
            }
            else {
                handleError(conn);
            }
            return null;
        }
    }

    /**
     * Get the info of a workflow job.
     *
     * @param jobId job Id.
     * @return the job info.
     * @throws OozieClientException thrown if the job info could not be retrieved.
     */
    public WorkflowJob getJobInfo(String jobId) throws OozieClientException {
        return new JobInfo(jobId).call();
    }

    private class JobsStatus extends ClientCallable<List<WorkflowJob>> {

        JobsStatus(String filter, int start, int len) {
            super("GET", RestConstants.JOBS, "", prepareParams(RestConstants.JOBS_FILTER_PARAM, filter,
                                                               RestConstants.OFFSET_PARAM, Integer.toString(start),
                                                               RestConstants.LEN_PARAM, Integer.toString(len)));
        }

        @SuppressWarnings("unchecked")
        protected List<WorkflowJob> call(HttpURLConnection conn) throws IOException, OozieClientException {
            if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                Reader reader = new InputStreamReader(conn.getInputStream());
                JSONObject json = (JSONObject) JSONValue.parse(reader);
                JSONArray workflows = (JSONArray) json.get(JsonTags.WORKFLOWS_JOBS);
                return JsonWorkflowJob.fromJSONArray(workflows);
            }
            else {
                handleError(conn);
            }
            return null;
        }
    }

    /**
     * Return the info of the workflow jobs that match the filter.
     *
     * @param filter job filter. Refer to the {@link OozieClient} for the filter syntax.
     * @param start jobs offset, base 1.
     * @param len number of jobs to return.
     * @return a list with the workflow jobs info, without node details.
     * @throws OozieClientException thrown if the jobs info could not be retrieved.
     */
    public List<WorkflowJob> getJobsInfo(String filter, int start, int len) throws OozieClientException {
        return new JobsStatus(filter, start, len).call();
    }

    /**
     * Return the info of the workflow jobs that match the filter.
     * <p/>
     * It returns the first 100 jobs that match the filter.
     *
     * @param filter job filter. Refer to the {@link OozieClient} for the filter syntax.
     * @return a list with the workflow jobs info, without node details.
     * @throws OozieClientException thrown if the jobs info could not be retrieved.
     */
    public List<WorkflowJob> getJobsInfo(String filter) throws OozieClientException {
        return getJobsInfo(filter, 1, 50);
    }

    private class JobIdAction extends ClientCallable<String> {

        JobIdAction(String externalId) {
            super("GET", RestConstants.JOBS, "", prepareParams(RestConstants.JOBS_EXTERNAL_ID_PARAM, externalId));
        }

        @SuppressWarnings("unchecked")
        protected String call(HttpURLConnection conn) throws IOException, OozieClientException {
            if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                Reader reader = new InputStreamReader(conn.getInputStream());
                JSONObject json = (JSONObject) JSONValue.parse(reader);
                return (String) json.get(JsonTags.JOB_ID);
            }
            else {
                handleError(conn);
            }
            return null;
        }
    }

    /**
     * Return the workflow job Id for an external Id.
     * <p/>
     * The external Id must have provided at job creation time.
     *
     * @param externalId external Id given at job creation time.
     * @return the workflow job Id for an external Id, <code>null</code> if none.
     * @throws OozieClientException thrown if the operation could not be done.
     */
    public String getJobId(String externalId) throws OozieClientException {
        return new JobIdAction(externalId).call();
    }

    private class SetSafeMode extends ClientCallable<Void>{

        public SetSafeMode(boolean status) {
            super("PUT", RestConstants.ADMIN, RestConstants.ADMIN_STATUS_RESOURCE,
                  prepareParams(RestConstants.ADMIN_SAFE_MODE_PARAM, status+""));
        }
        
        public Void call(HttpURLConnection conn) throws IOException, OozieClientException {
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                handleError(conn);
            }
            return null;
        }
    }
    
    /**
     * Enable or disable safe mode. Used by OozieCLI.
     * 
     * In safe mode, Oozie would not accept any commands except status command to 
     * change and view the safe mode status.
     * 
     * @param status true to enable safe mode, false to disable safe mode.
     * @throws OozieClientException if it fails to set the safe mode status.
     */
    public void setSafeMode(boolean status) throws OozieClientException {
        new SetSafeMode(status).call();
    }

    private class GetSafeMode extends ClientCallable<Boolean> {

        GetSafeMode() {
            super("GET", RestConstants.ADMIN, RestConstants.ADMIN_STATUS_RESOURCE, prepareParams());
        }

        protected Boolean call(HttpURLConnection conn) throws IOException, OozieClientException {
            if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                Reader reader = new InputStreamReader(conn.getInputStream());
                JSONObject json = (JSONObject) JSONValue.parse(reader);
                return (Boolean)json.get(JsonTags.SYSTEM_SAFE_MODE);
            }
            else {
                handleError(conn);
            }
            return true;
        }
    }

    /**
     * Returns if Oozie is in safe mode or not.
     * 
     * @return true if safe mode is ON<br> false if safe mode is OFF 
     * @throws OozieClientException throw if it could not obtain the safe mode status.
     */
    public boolean isInSafeMode() throws OozieClientException {
        return new GetSafeMode().call();
    }

    private class GetBuildVersion extends ClientCallable<String> {

        GetBuildVersion() {
            super("GET", RestConstants.ADMIN, RestConstants.ADMIN_BUILD_VERSION_RESOURCE, prepareParams());
        }

        protected String call(HttpURLConnection conn) throws IOException, OozieClientException {
            if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                Reader reader = new InputStreamReader(conn.getInputStream());
                JSONObject json = (JSONObject) JSONValue.parse(reader);
                return (String)json.get(JsonTags.BUILD_VERSION);
            }
            else {
                handleError(conn);
            }
            return null;
        }
    }

    /**
     * Return the Oozie server build version.
     *
     * @return the Oozie server build version.
     * @throws OozieClientException throw if it the server build version could not be retrieved.
     */
    public String getServerBuildVersion() throws OozieClientException {
        return new GetBuildVersion().call();
    }

    /**
     * Return the Oozie client build version.
     *
     * @return the Oozie client build version.
     */
    public String getClientBuildVersion() {
        return BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION);
    }

}
