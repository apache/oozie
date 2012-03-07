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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.coord.SLAEventsXCommand;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

public class SLAServlet extends JsonRestServlet {
    private static final String INSTRUMENTATION_NAME = "sla";

    private static final JsonRestServlet.ResourceInfo RESOURCES_INFO[] = new JsonRestServlet.ResourceInfo[1];

    static {
        RESOURCES_INFO[0] = new JsonRestServlet.ResourceInfo("", Arrays
                .asList("GET"), Arrays.asList(
                new JsonRestServlet.ParameterInfo(
                        RestConstants.SLA_GT_SEQUENCE_ID, String.class, true,
                        Arrays.asList("GET")),
                new JsonRestServlet.ParameterInfo(RestConstants.MAX_EVENTS,
                                                  String.class, false, Arrays.asList("GET"))));
    }

    public SLAServlet() {
        super(INSTRUMENTATION_NAME, RESOURCES_INFO);
    }

    /**
     * Return information about SLA Events.
     */
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        try {
            String gtSequenceNum = request
                    .getParameter(RestConstants.SLA_GT_SEQUENCE_ID);
            String strMaxEvents = request
                    .getParameter(RestConstants.MAX_EVENTS);
            int maxNoEvents = 100; // Default
            XLog.getLog(getClass()).debug(
                    "Got SLA GET request for :" + gtSequenceNum
                            + " and max-events :" + strMaxEvents);
            if (strMaxEvents != null && strMaxEvents.length() > 0) {
                maxNoEvents = Integer.parseInt(strMaxEvents);
            }
            if (gtSequenceNum != null) {
                long seqId = Long.parseLong(gtSequenceNum);
                stopCron();
                SLAEventsXCommand seCommand = new SLAEventsXCommand(seqId, maxNoEvents);
                List<SLAEventBean> slaEvntList = seCommand.call();
                long lastSeqId = seCommand.getLastSeqId();

                Element eResponse = new Element("sla-message");
                for (SLAEventBean event : slaEvntList) {
                    eResponse.addContent(event.toXml());
                }
                Element eLastSeq = new Element("last-sequence-id");
                eLastSeq.addContent(String.valueOf(lastSeqId));
                eResponse.addContent(eLastSeq);
                response.setContentType(XML_UTF8);
                XLog.getLog(getClass()).debug("Writing back SLA Servlet  Caller with last-seq-id " + lastSeqId);
                startCron();
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(
                        XmlUtils.prettyPrint(eResponse) + "\n");
            }
            else {
                XLog.getLog(getClass()).error(
                        "Not implemented witout gt_seq_id");
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST,
                                            ErrorCode.E0401, "Not implemented without gtSeqID");
            }
        }
        catch (CommandException ce) {
            ce.printStackTrace();
            XLog.getLog(getClass()).error("Command exception ", ce);
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ce);
        }
        catch (RuntimeException re) {
            re.printStackTrace();
            XLog.getLog(getClass()).error("Runtime error ", re);
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0307, re.getMessage());
        }
    }

}
