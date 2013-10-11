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
package org.apache.oozie.service;

import java.io.BufferedReader;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLogStreamer;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.util.SimpleTimestampedMessageParser;
import org.apache.oozie.util.TimestampedMessageParser;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ZKUtils;

/**
 * Service that performs streaming of log files over Web Services if enabled in XLogService and collates logs from other Oozie
 * servers.  Requires that a ZooKeeper ensemble is available.
 */
public class ZKXLogStreamingService extends XLogStreamingService implements Service, Instrumentable {

    private static final String ALL_SERVERS_PARAM = "allservers";

    private ZKUtils zk;
    private XLog log;
    private Class<? extends Authenticator> AuthenticatorClass;

    /**
     * Initialize the log streaming service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the log streaming service could not be initialized.
     */
    @Override
    public void init(Services services) throws ServiceException {
        super.init(services);
        try {
            zk = ZKUtils.register(this);
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E1700, ex.getMessage(), ex);
        }
        log = XLog.getLog(this.getClass());
        try {
            AuthenticatorClass = determineAuthenticatorClassType();
        } catch (Exception ex) {
            throw new ServiceException(ErrorCode.E0100, ex);
        }
    }

    /**
     * Destroy the log streaming service.
     */
    @Override
    public void destroy() {
        if (zk != null) {
            zk.unregister(this);
        }
        zk = null;
        super.destroy();
    }

    /**
     * Instruments the log streaming service.
     *
     * @param instr instrumentation to use.
     */
    @Override
    public void instrument(Instrumentation instr) {
        super.instrument(instr);
    }

    /**
     * Stream the log of a job.  It contacts any other running Oozie servers to collate relevant logs while streaming.
     *
     * @param filter log streamer filter.
     * @param startTime start time for log events to filter.
     * @param endTime end time for log events to filter.
     * @param writer writer to stream the log to.
     * @param params additional parameters from the request
     * @throws IOException thrown if the log cannot be streamed.
     */
    @Override
    public void streamLog(XLogStreamer.Filter filter, Date startTime, Date endTime, Writer writer, Map<String, String[]> params)
            throws IOException {
        XLogService xLogService = Services.get().get(XLogService.class);
        if (xLogService.getLogOverWS()) {
            // If ALL_SERVERS_PARAM is set to false, then only stream our log
            if (params.get(ALL_SERVERS_PARAM) != null && params.get(ALL_SERVERS_PARAM).length > 0
                    && params.get(ALL_SERVERS_PARAM)[0].equals("false")) {
                new XLogStreamer(filter, xLogService.getOozieLogPath(), xLogService.getOozieLogName(),
                        xLogService.getOozieLogRotation()).streamLog(writer, startTime, endTime);
            }
            // Otherwise, we have to go collate relevant logs from the other Oozie servers
            else {
                collateLogs(filter, startTime, endTime, writer);
            }
        }
        else {
            writer.write("Log streaming disabled!!");
        }
    }

    /**
     * Contacts each of the other Oozie servers, gets their logs for the job, collates them, and sends them to the user via the
     * Writer.  It will make sure to not read all of the log messages into memory at the same time to not use up the heap.  If there
     * is a problem talking to one of the other servers, it will ignore that server and prepend a message to the Writer about it.
     * For getting the logs from this server, it won't use the REST API and instead get them directly to be more efficient.
     *
     * @param filter
     * @param startTime
     * @param endTime
     * @param writer
     * @throws IOException
     */
    private void collateLogs(XLogStreamer.Filter filter, Date startTime, Date endTime, Writer writer) throws IOException {
        XLogService xLogService = Services.get().get(XLogService.class);
        List<String> badOozies = new ArrayList<String>();
        List<ServiceInstance<Map>> oozies = null;
        try {
            oozies = zk.getAllMetaData();
        }
        catch (Exception ex) {
            throw new IOException("Issue communicating with ZooKeeper: " + ex.getMessage(), ex);
        }
        List<TimestampedMessageParser> parsers = new ArrayList<TimestampedMessageParser>(oozies.size());
        try {
            // Create a BufferedReader for getting the logs of each server and put them in a TimestampedMessageParser
            for (ServiceInstance<Map> oozie : oozies) {
                Map<String, String> oozieMeta = oozie.getPayload();
                String otherId = oozieMeta.get(ZKUtils.ZKMetadataKeys.OOZIE_ID);
                // If it's this server, we can just get them directly
                if (otherId.equals(zk.getZKId())) {
                    BufferedReader reader = new XLogStreamer(filter, xLogService.getOozieLogPath(), xLogService.getOozieLogName(),
                                                             xLogService.getOozieLogRotation()).makeReader(startTime, endTime);
                    parsers.add(new TimestampedMessageParser(reader, filter));
                }
                // If it's another server, we'll have to use the REST API
                else {
                    String otherUrl = oozieMeta.get(ZKUtils.ZKMetadataKeys.OOZIE_URL);
                    String jobId = filter.getFilterParams().get(DagXLogInfoService.JOB);
                    try {
                        BufferedReader reader = fetchOtherLog(otherUrl, jobId);
                        parsers.add(new SimpleTimestampedMessageParser(reader, filter));
                    }
                    catch(IOException ioe) {
                        log.warn("Failed to retrieve logs for job [" + jobId + "] from Oozie server with ID [" + otherId
                                + "] at [" + otherUrl + "]; log information may be incomplete", ioe);
                        badOozies.add(otherId);
                    }
                }
            }

            // Add a message about any servers we couldn't contact
            if (!badOozies.isEmpty()) {
                writer.write("Unable to contact the following Oozie Servers for logs (log information may be incomplete):\n");
                for (String badOozie : badOozies) {
                    writer.write("     ");
                    writer.write(badOozie);
                    writer.write("\n");
                }
                writer.write("\n");
                writer.flush();
            }

            // If it's just the one server (this server), then we don't need to do any more processing and can just copy it directly
            if (parsers.size() == 1) {
                TimestampedMessageParser parser = parsers.get(0);
                parser.processRemaining(writer);
            }
            else {
                // Now that we have a Reader for each server to get the logs from that server, we have to collate them.  Within each
                // server, the logs should already be in the correct order, so we can take advantage of that.  We'll use the
                // BufferedReaders to read the messages from the logs of each server and put them in order without having to bring
                // every message into memory at the same time.
                TreeMap<String, TimestampedMessageParser> timestampMap = new TreeMap<String, TimestampedMessageParser>();
                // populate timestampMap with initial values
                for (TimestampedMessageParser parser : parsers) {
                    if (parser.increment()) {
                        timestampMap.put(parser.getLastTimestamp(), parser);
                    }
                }
                while (timestampMap.size() > 1) {
                    // The first entry will be the earliest based on the timestamp (also removes it) from the map
                    TimestampedMessageParser earliestParser = timestampMap.pollFirstEntry().getValue();
                    // Write the message from that parser at that timestamp
                    writer.write(earliestParser.getLastMessage());
                    // Increment that parser to read the next message
                    if (earliestParser.increment()) {
                        // If it still has messages left, put it back in the map with the new last timestamp for it
                        timestampMap.put(earliestParser.getLastTimestamp(), earliestParser);
                    }
                }
                // If there's only one parser left in the map, then we can simply copy the rest of its lines directly to be faster
                if (timestampMap.size() == 1) {
                    TimestampedMessageParser parser = timestampMap.values().iterator().next();
                    writer.write(parser.getLastMessage());  // don't forget the last message read by the parser
                    parser.processRemaining(writer);
                }
            }
        }
        finally {
            for (TimestampedMessageParser parser : parsers) {
                parser.closeReader();
            }
            writer.flush();
        }
    }

    /**
     * Creates a connection over HTTP to another Oozie server and asks it for the logs related to the jobId.
     *
     * @param otherUrl The URL of the other Oozie server
     * @param jobId The job id of the job we want logs for
     * @return a BufferedReader of the logs from the other server for the jobId
     * @throws IOException If there was a problem connecting to the other Oozie server
     */
    private BufferedReader fetchOtherLog(String otherUrl, String jobId) throws IOException {
        // It's important that we specify ALL_SERVERS_PARAM=false in the GET request to prevent the other Oozie Server from trying
        // aggregate logs from the other Oozie servers (and creating an infinite recursion)
        final URL url = new URL(otherUrl + "/v" + OozieClient.WS_PROTOCOL_VERSION + "/" + RestConstants.JOB + "/" + jobId
                + "?" + RestConstants.JOB_SHOW_PARAM + "=" + RestConstants.JOB_SHOW_LOG + "&" + ALL_SERVERS_PARAM + "=false");

        log.debug("Fetching logs from [{0}]", url);
        BufferedReader reader = null;
        try {
            reader = UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<BufferedReader>() {
                @Override
                public BufferedReader run() throws IOException {
                    HttpURLConnection conn = getConnection(url);
                    BufferedReader reader = null;
                    if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                        InputStream is = conn.getInputStream();
                        reader = new BufferedReader(new InputStreamReader(is));
                    }
                    return reader;
                }
            });
        }
        catch (InterruptedException ie) {
            throw new IOException(ie);
        }
        return reader;
    }

    private HttpURLConnection getConnection(URL url) throws IOException {
        AuthenticatedURL.Token token = new AuthenticatedURL.Token();
        HttpURLConnection conn;
        try {
            conn = new AuthenticatedURL(AuthenticatorClass.newInstance()).openConnection(url, token);
        }
        catch (AuthenticationException ex) {
            throw new IOException("Could not authenticate, " + ex.getMessage(), ex);
        } catch (InstantiationException ex) {
            throw new IOException("Could not authenticate, " + ex.getMessage(), ex);
        } catch (IllegalAccessException ex) {
            throw new IOException("Could not authenticate, " + ex.getMessage(), ex);
        }
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            throw new IOException("Unexpected response code [" + conn.getResponseCode() + "], message [" + conn.getResponseMessage()
                    + "]");
        }
        return conn;
    }

    @SuppressWarnings("unchecked")
    private Class<? extends Authenticator> determineAuthenticatorClassType() throws Exception {
        // Adapted from org.apache.hadoop.security.authentication.server.AuthenticationFilter#init
        Class<? extends Authenticator> authClass;
        String authName = Services.get().getConf().get("oozie.authentication.type");
        String authClassName;
        if (authName == null) {
            throw new IOException("Authentication type must be specified: simple|kerberos|<class>");
        }
        authName = authName.trim();
        if (authName.equals("simple")) {
            authClassName = PseudoAuthenticator.class.getName();
        } else if (authName.equals("kerberos")) {
            authClassName = KerberosAuthenticator.class.getName();
        } else {
            authClassName = authName;
        }

        authClass = (Class<? extends Authenticator>) Thread.currentThread().getContextClassLoader().loadClass(authClassName);
        return authClass;
    }
}
