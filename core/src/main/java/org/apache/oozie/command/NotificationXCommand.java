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
package org.apache.oozie.command;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;

public abstract class NotificationXCommand extends XCommand<Void> {

    public static final String NOTIFICATION_URL_CONNECTION_TIMEOUT_KEY = "oozie.notification.url.connection.timeout";
    public static final String NOTIFICATION_PROXY_KEY = "oozie.notification.proxy";
    private static final int MAX_RETRIES = 3;

    protected int retries = 0;
    protected String jobId;
    protected String url;
    protected String proxyConf;

    public NotificationXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    @Override
    final protected boolean isLockRequired() {
        return false;
    }

    @Override
    public String getEntityKey() {
        return jobId;
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {

    }

    @Override
    protected Void execute() throws CommandException {
        sendNotification();
        return null;
    }

    @Override
    protected void setLogInfo() {
        LogUtils.setLogInfo(jobId);
    }

    protected Proxy getProxy(String proxyConf) {
        // Configure the proxy to use if its set. It should be set like
        // proxyType@proxyHostname:port
        if (proxyConf != null && !proxyConf.trim().equals("") && proxyConf.lastIndexOf(":") != -1) {
            int typeIndex = proxyConf.indexOf("@");
            Proxy.Type proxyType = Proxy.Type.HTTP;
            if (typeIndex != -1 && proxyConf.substring(0, typeIndex).compareToIgnoreCase("socks") == 0) {
                proxyType = Proxy.Type.SOCKS;
            }
            String hostname = proxyConf.substring(typeIndex + 1, proxyConf.lastIndexOf(":"));
            String portConf = proxyConf.substring(proxyConf.lastIndexOf(":") + 1);
            try {
                int port = Integer.parseInt(portConf);
                LOG.info("Workflow notification using proxy type \"" + proxyType + "\" hostname \"" + hostname
                        + "\" and port \"" + port + "\"");
                return new Proxy(proxyType, new InetSocketAddress(hostname, port));
            }
            catch (NumberFormatException nfe) {
                LOG.warn("Workflow notification couldn't parse configured proxy's port " + portConf
                        + ". Not going to use a proxy");
            }
        }
        return Proxy.NO_PROXY;
    }

    protected void handleRetry() {
        if (retries < MAX_RETRIES) {
            retries++;
            this.resetUsed();
            queue(this, 60 * 1000);
        }
        else {
            LOG.warn(XLog.OPS, "could not send notification [{0}]", url);
        }
    }

    protected void sendNotification() {
        if (url != null) {
            Proxy proxy = getProxy(proxyConf);
            try {
                LOG.debug("Trying to send notification to [{0}] for the [{1}] time out of total [{2}]",
                        url, retries, MAX_RETRIES);
                URL url = new URL(this.url);
                HttpURLConnection urlConn = (HttpURLConnection) url.openConnection(proxy);
                int timeout = getTimeOut();
                urlConn.setConnectTimeout(timeout);
                urlConn.setReadTimeout(timeout);
                LOG.debug("Setting connection timeout and read timeout to [{0}] ms", timeout);
                int responseCode = urlConn.getResponseCode();
                if (responseCode != HttpURLConnection.HTTP_OK) {
                    LOG.warn("Notification sending to URL [{0}] failed. response code: [{1}], response message: [{2}]",
                            this.url, responseCode, urlConn.getResponseMessage());
                    handleRetry();
                } else {
                    LOG.debug("Notification sent to [{0}]", this.url);
                }
            }
            catch (IOException ex) {
                LOG.warn("Notification sending to URL [{0}] failed", url, ex);
                handleRetry();
            }
        }
        else {
            LOG.info("No Notification URL is defined. Therefore nothing to notify for job " + jobId);

        }

    }

    private int getTimeOut() {
        return ConfigurationService.getInt(NOTIFICATION_URL_CONNECTION_TIMEOUT_KEY);
    }

    public void setRetry(int retries) {
        this.retries = retries;

    }

}
