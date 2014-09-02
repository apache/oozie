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

package org.apache.oozie.jms;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

public class JMSExceptionListener implements ExceptionListener {

    private static XLog LOG = XLog.getLog(JMSExceptionListener.class);
    private JMSConnectionInfo connInfo;
    private ConnectionContext connCtxt;
    private boolean retry;

    /**
     * Create ExceptionLister for a JMS Connection
     *
     * @param connInfo Information to connect to the JMS compliant messaging service
     * @param connCtxt The actual connection on which this listener will be registered
     */
    public JMSExceptionListener(JMSConnectionInfo connInfo, ConnectionContext connCtxt, boolean retry) {
        this.connInfo = connInfo;
        this.connCtxt = connCtxt;
        this.retry = retry;
    }

    @Override
    public void onException(JMSException exception) {
        LOG.warn("Received JMSException for [{0}]", connInfo, exception);
        connCtxt.close();
        if (retry) {
            JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);
            jmsService.reestablishConnection(connInfo);
        }
    }

}
