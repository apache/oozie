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
import java.io.IOException;
import java.io.Writer;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.util.AuthUrlClient;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.SimpleTimestampedMessageParser;
import org.apache.oozie.util.TimestampedMessageParser;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XLogStreamer;
import org.apache.oozie.util.ZKUtils;

/**
 * Service that performs streaming of log files over Web Services if enabled in XLogService and collates logs from other Oozie
 * servers.  Requires that a ZooKeeper ensemble is available.
 */
public class ZKXLogStreamingService extends XLogStreamingService implements Service, Instrumentable {

    private ZKUtils zk;
    private XLog log;

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
     * @param logStreamer the log streamer
     * @param startTime start time for log events to filter.
     * @param endTime end time for log events to filter.
     * @param writer writer to stream the log to.
     * @throws IOException thrown if the log cannot be streamed.
     */
    @Override
    public void streamLog(XLogStreamer logStreamer, Date startTime, Date endTime, Writer writer) throws IOException {

        if (!logStreamer.isLogEnabled()) {
            writer.write(StringEscapeUtils.escapeHtml(logStreamer.getLogDisableMessage()));
            return;
        }
        // If ALL_SERVERS_PARAM is set to false, then only stream our log
        if (!Services.get().get(JobsConcurrencyService.class).isAllServerRequest(logStreamer.getRequestParam())) {
            super.streamLog(logStreamer, startTime, endTime, writer, false);
        }
        // Otherwise, we have to go collate relevant logs from the other Oozie servers
        else {
            collateLogs(logStreamer, startTime, endTime, writer);
        }
    }

    /**
     * Contacts each of the other Oozie servers, gets their logs for the job, collates them, and sends them to the user via the
     * Writer.  It will make sure to not read all of the log messages into memory at the same time to not use up the heap.  If there
     * is a problem talking to one of the other servers, it will ignore that server and prepend a message to the Writer about it.
     * For getting the logs from this server, it won't use the REST API and instead get them directly to be more efficient.
     *
     * @param logStreamer the XLogStreamer
     * @param startTime the job start time
     * @param endTime the job end time
     * @param writer the writer
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void collateLogs(XLogStreamer logStreamer, Date startTime, Date endTime, Writer writer) throws IOException {
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
                    BufferedReader reader = logStreamer.makeReader(startTime,
                            endTime);
                    parsers.add(new TimestampedMessageParser(reader, logStreamer.getXLogFilter()));
                }
                // If it's another server, we'll have to use the REST API
                else {
                    String otherUrl = oozieMeta.get(ZKUtils.ZKMetadataKeys.OOZIE_URL);
                    String jobId = logStreamer.getXLogFilter().getFilterParams().get(DagXLogInfoService.JOB);
                    try {
                     // It's important that we specify ALL_SERVERS_PARAM=false in the GET request to prevent the other Oozie
                     // Server from trying aggregate logs from the other Oozie servers (and creating an infinite recursion)
                        final String url = otherUrl + "/v" + OozieClient.WS_PROTOCOL_VERSION + "/" + RestConstants.JOB
                                + "/" + jobId + "?" + RestConstants.JOB_SHOW_PARAM + "=" + logStreamer.getLogType()
                                + "&" + RestConstants.ALL_SERVER_REQUEST + "=false"
                                + AuthUrlClient.getQueryParamString(logStreamer.getRequestParam());
                        // remove doAs from url to avoid failure while fetching
                        // logs in case of HA mode
                        String key = "doAs";
                        String[] value = null;
                        if (logStreamer.getRequestParam() != null) {
                            value = logStreamer.getRequestParam().get(key);
                        }
                        String urlWithoutdoAs = null;
                        if (value != null && value.length > 0 && value[0] != null && value[0].length() > 0) {
                            urlWithoutdoAs = url.replace("&" + key + "=" + URLEncoder.encode(value[0], "UTF-8"), "");
                        }
                        else {
                            urlWithoutdoAs = url;
                        }
                        BufferedReader reader = AuthUrlClient.callServer(urlWithoutdoAs);
                        parsers.add(new SimpleTimestampedMessageParser(reader, logStreamer.getXLogFilter()));
                    }
                    catch(IOException ioe) {
                        log.warn("Failed to retrieve logs for job [" + jobId + "] from Oozie server with ID [" + otherId
                                + "] at [" + otherUrl + "]; log information may be incomplete", ioe);
                        badOozies.add(otherId);
                    }
                }
            }

            //If log param debug is set, we need to write start date and end date to outputstream.
            if(!StringUtils.isEmpty(logStreamer.getXLogFilter().getTruncatedMessage())){
                writer.write(StringEscapeUtils.escapeHtml(logStreamer.getXLogFilter().getTruncatedMessage()));
            }

            if (logStreamer.getXLogFilter().isDebugMode()) {
                writer.write(StringEscapeUtils.escapeHtml(logStreamer.getXLogFilter().getDebugMessage()));
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
                parser.processRemaining(writer, logStreamer);
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
                    writer.write(StringEscapeUtils.escapeHtml(earliestParser.getLastMessage()));
                    if (logStreamer.shouldFlushOutput(earliestParser.getLastMessage().length())) {
                        writer.flush();
                    }
                    // Increment that parser to read the next message
                    if (earliestParser.increment()) {
                        // If it still has messages left, put it back in the map with the new last timestamp for it
                        timestampMap.put(earliestParser.getLastTimestamp(), earliestParser);
                    }
                }
                // If there's only one parser left in the map, then we can simply copy the rest of its lines directly to be faster
                if (timestampMap.size() == 1) {
                    TimestampedMessageParser parser = timestampMap.values().iterator().next();
                    // don't forget the last message read by the parser
                    writer.write(StringEscapeUtils.escapeHtml(parser.getLastMessage()));
                    parser.processRemaining(writer, logStreamer);
                }
            }
        }
        finally {
            for (TimestampedMessageParser parser : parsers) {
                parser.closeReader();
            }
        }
    }
}
