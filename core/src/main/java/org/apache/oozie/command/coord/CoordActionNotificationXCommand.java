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
package org.apache.oozie.command.coord;

import java.io.IOException;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.wf.NotificationXCommand;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

/**
 * This class will send the notification for the coordinator action
 */
public class CoordActionNotificationXCommand extends CoordinatorXCommand<Void> {

    private final CoordinatorActionBean actionBean;
    private static final String STATUS_PATTERN = "\\$status";
    private static final String ACTION_ID_PATTERN = "\\$actionId";

    //this variable is package private only for test purposes
    int retries = 0;

    public CoordActionNotificationXCommand(CoordinatorActionBean actionBean) {
        super("coord_action_notification", "coord_action_notification", 0);
        ParamChecker.notNull(actionBean, "Action Bean");
        this.actionBean = actionBean;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        LOG.info("STARTED Coordinator Notification actionId=" + actionBean.getId() + " : " + actionBean.getStatus());
        Configuration conf;
        try {
            conf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        }
        catch (IOException e1) {
            LOG.warn("Configuration parse error. read from DB :" + actionBean.getRunConf());
            throw new CommandException(ErrorCode.E1005, e1.getMessage(), e1);
        }
        String url = conf.get(OozieClient.COORD_ACTION_NOTIFICATION_URL);
        if (url != null) {
            url = url.replaceAll(ACTION_ID_PATTERN, actionBean.getId());
            url = url.replaceAll(STATUS_PATTERN, actionBean.getStatus().toString());
            LOG.debug("Notification URL :" + url);
            try {
                int timeout = Services.get().getConf().getInt(
                    NotificationXCommand.NOTIFICATION_URL_CONNECTION_TIMEOUT_KEY,
                    NotificationXCommand.NOTIFICATION_URL_CONNECTION_TIMEOUT_DEFAULT);
                URL urlObj = new URL(url);
                HttpURLConnection urlConn = (HttpURLConnection) urlObj.openConnection();
                urlConn.setConnectTimeout(timeout);
                urlConn.setReadTimeout(timeout);
                if (urlConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                    handleRetry(url);
                }
            }
            catch (IOException ex) {
                handleRetry(url);
            }
        }
        else {
            LOG.info("No Notification URL is defined. Therefore nothing to notify for job " + actionBean.getJobId()
                    + " action ID " + actionBean.getId());
        }
        LOG.info("ENDED Coordinator Notification actionId=" + actionBean.getId());
        return null;
    }

    /**
     * This method handles the retry for the coordinator action.
     *
     * @param url This is the URL where the notification has to be sent.
     */
    private void handleRetry(String url) {
        if (retries < 3) {
            retries++;
            this.resetUsed();
            queue(this, 60 * 1000);
        }
        else {
            LOG.warn(XLog.OPS, "could not send notification [{0}]", url);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return actionBean.getId();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        LogUtils.setLogInfo(actionBean, logInfo);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }
}
