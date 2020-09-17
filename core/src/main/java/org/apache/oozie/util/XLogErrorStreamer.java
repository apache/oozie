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

package org.apache.oozie.util;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;

public class XLogErrorStreamer extends XLogStreamer {

    public static final String STREAM_BUFFER_LEN = CONF_PREFIX + "error.buffer.len";

    public XLogErrorStreamer(XLogFilter logFilter, Map<String, String[]> requestParameters) {
        super(logFilter, Services.get().get(XLogService.class).getOozieErrorLogPath(), Services.get()
                .get(XLogService.class).getOozieErrorLogName(), Services.get().get(XLogService.class)
                .getOozieErrorLogRotation());
        this.requestParam = requestParameters;
        bufferLen = ConfigurationService.getInt(STREAM_BUFFER_LEN, 2048);
    }

    public XLogErrorStreamer(Map<String, String[]> requestParameters) throws CommandException {
        this(new XLogFilter(new XLogUserFilterParam(requestParameters)), requestParameters);
    }

    @Override
    protected void calculateAndValidateDateRange(Date startTime, Date endTime) throws IOException {
        logFilter.calculateAndCheckDates(startTime, endTime);
        // no validate and truncating
    }

    @Override
    public boolean isLogEnabled() {
        return Services.get().get(XLogService.class).isErrorLogEnabled();
    }

    @Override
    public String getLogType() {
        return RestConstants.JOB_SHOW_ERROR_LOG;
    }

    @Override
    public String getLogDisableMessage() {
        return "Error Log is disabled!!";
    }
}
