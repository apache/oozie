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

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLogStreamer;
import java.io.IOException;
import java.io.Writer;
import java.util.Date;

/**
 * Service that performs streaming of log files over Web Services if enabled in XLogService
 */
public class XLogStreamingService implements Service, Instrumentable {


    /**
     * Initialize the log streaming service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the log streaming service could not be initialized.
     */
    public void init(Services services) throws ServiceException {
    }

    /**
     * Destroy the log streaming service.
     */
    public void destroy() {
    }

    /**
     * Return the public interface for log streaming service.
     *
     * @return {@link XLogStreamingService}.
     */
    public Class<? extends Service> getInterface() {
        return XLogStreamingService.class;
    }

    /**
     * Instruments the log streaming service.
     *
     * @param instr instrumentation to use.
     */
    public void instrument(Instrumentation instr) {
        // nothing to instrument
    }


    /**
     * Stream the log of a job.
     *
     * @param logStreamer the log streamer
     * @param startTime start time for log events to filter.
     * @param endTime end time for log events to filter.
     * @param writer writer to stream the log to.
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void streamLog(XLogStreamer logStreamer, Date startTime, Date endTime, Writer writer) throws IOException {
        streamLog(logStreamer, startTime, endTime, writer, true);
    }

    /**
     * Stream log.
     *
     * @param logStreamer the log streamer
     * @param startTime the start time
     * @param endTime the end time
     * @param writer the writer
     * @param appendDebug the append debug
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected void streamLog(XLogStreamer logStreamer, Date startTime, Date endTime, Writer writer, boolean appendDebug)
            throws IOException {
        if (!logStreamer.isLogEnabled()) {
            writer.write(StringEscapeUtils.escapeHtml(logStreamer.getLogDisableMessage()));
            return;
        }
        logStreamer.streamLog(writer, startTime, endTime, appendDebug);
    }
}
