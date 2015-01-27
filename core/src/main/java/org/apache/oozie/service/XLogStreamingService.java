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

import org.apache.oozie.util.XLogFilter;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLogStreamer;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Date;

/**
 * Service that performs streaming of log files over Web Services if enabled in XLogService
 */
public class XLogStreamingService implements Service, Instrumentable {
    private static final String CONF_PREFIX = Service.CONF_PREFIX + "XLogStreamingService.";
    public static final String STREAM_BUFFER_LEN = CONF_PREFIX + "buffer.len";

    protected int bufferLen;

    /**
     * Initialize the log streaming service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the log streaming service could not be initialized.
     */
    public void init(Services services) throws ServiceException {
        bufferLen = ConfigurationService.getInt(services.getConf(), STREAM_BUFFER_LEN);
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
     * @param filter log streamer filter.
     * @param startTime start time for log events to filter.
     * @param endTime end time for log events to filter.
     * @param writer writer to stream the log to.
     * @param params additional parameters from the request
     * @throws IOException thrown if the log cannot be streamed.
     */
    public void streamLog(XLogFilter filter, Date startTime, Date endTime, Writer writer, Map<String, String[]> params)
            throws IOException {
        XLogService xLogService = Services.get().get(XLogService.class);
        if (xLogService.getLogOverWS()) {
            new XLogStreamer(filter, xLogService.getOozieLogPath(), xLogService.getOozieLogName(),
                    xLogService.getOozieLogRotation()).streamLog(writer, startTime, endTime, bufferLen);
        }
        else {
            writer.write("Log streaming disabled!!");
        }
    }

    /**
     * Stream the error log of a job.
     *
     * @param filter log streamer filter.
     * @param startTime start time for log events to filter.
     * @param endTime end time for log events to filter.
     * @param writer writer to stream the log to.
     * @param params additional parameters from the request
     * @throws IOException thrown if the log cannot be streamed.
     */
    public void streamErrorLog(XLogFilter filter, Date startTime, Date endTime, Writer writer, Map<String, String[]> params)
            throws IOException {
        XLogService xLogService = Services.get().get(XLogService.class);
        if (xLogService.isErrorLogEnable()) {
            new XLogStreamer(filter, xLogService.getOozieErrorLogPath(), xLogService.getOozieErrorLogName(),
                    xLogService.getOozieErrorLogRotation()).streamLog(writer, startTime, endTime, bufferLen);
        }
        else {
            writer.write("Error Log is disabled!!");
        }
    }


    public int getBufferLen() {
        return bufferLen;
    }
}
