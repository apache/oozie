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
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.rest.JMSConnectionInfoBean;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.jms.JMSConnectionInfo;
import org.apache.oozie.jms.JMSJobEventListener;
import org.apache.oozie.service.JMSTopicService;
import org.apache.oozie.service.Services;

/**
 * V2 admin servlet
 *
 */
public class V2AdminServlet extends V1AdminServlet {

    private static final long serialVersionUID = 1L;
    private static final String INSTRUMENTATION_NAME = "v2admin";

    public V2AdminServlet() {
        super(INSTRUMENTATION_NAME);
    }

    @Override
    protected JsonBean getJMSConnectionInfo(HttpServletRequest request, HttpServletResponse response)
            throws XServletException, IOException {
        Configuration conf = Services.get().getConf();
        JMSTopicService jmsTopicService = Services.get().get(JMSTopicService.class);
        String connectionProperties = conf.get(JMSJobEventListener.JMS_CONNECTION_PROPERTIES);
        if (connectionProperties == null) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E1601,
                    "JMS connection property is not defined");
        }
        JMSConnectionInfoBean jmsBean = new JMSConnectionInfoBean();
        JMSConnectionInfo jmsInfo = new JMSConnectionInfo(connectionProperties);
        Properties jmsInfoProps = jmsInfo.getJNDIProperties();
        jmsInfoProps.remove("java.naming.security.principal");
        jmsBean.setJNDIProperties(jmsInfoProps);
        if (jmsTopicService != null) {
            jmsBean.setTopicPrefix(jmsTopicService.getTopicPrefix());
            jmsBean.setTopicPatternProperties(jmsTopicService.getTopicPatternProperties());
        }
        else {
            throw new XServletException(
                    HttpServletResponse.SC_BAD_REQUEST,
                    ErrorCode.E1601,
                    "JMSTopicService is not initialized. JMS notification"
                            + "may not be enabled");
        }
        return jmsBean;
    }

}
