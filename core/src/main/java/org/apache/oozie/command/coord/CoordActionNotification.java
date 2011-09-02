/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
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
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

public class CoordActionNotification extends CoordinatorCommand<Void> {

    private CoordinatorActionBean actionBean;
    private static final String STATUS_PATTERN = "\\$status";
    private static final String ACTION_ID_PATTERN = "\\$actionId";

    private int retries = 0;
    private final XLog log = XLog.getLog(getClass());

    public CoordActionNotification(CoordinatorActionBean actionBean) {
        super("coord_action_notification", "coord_action_notification", 0,
                XLog.STD);
        this.actionBean = actionBean;
    }

    @Override
    protected Void call(CoordinatorStore store) throws StoreException,
            CommandException {
        setLogInfo(actionBean);
        log.info("STARTED Coordinator Notification actionId="
                + actionBean.getId() + " : " + actionBean.getStatus());
        Configuration conf;
        try {
            conf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        }
        catch (IOException e1) {
            log.warn("Configuration parse error. read from DB :"
                    + actionBean.getRunConf());
            throw new CommandException(ErrorCode.E1005, e1.getMessage(), e1);
        }
        String url = conf.get(OozieClient.COORD_ACTION_NOTIFICATION_URL);
        if (url != null) {
            url = url.replaceAll(ACTION_ID_PATTERN, actionBean.getId());
            url = url.replaceAll(STATUS_PATTERN, actionBean.getStatus()
                    .toString());
            log.debug("Notification URL :" + url);
            try {
                URL urlObj = new URL(url);
                HttpURLConnection urlConn = (HttpURLConnection) urlObj
                        .openConnection();
                if (urlConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                    handleRetry(url);
                }
            }
            catch (IOException ex) {
                handleRetry(url);
            }
        }
        else {
            log
                    .info("No Notification URL is defined. Therefore nothing to notify for job "
                            + actionBean.getJobId()
                            + " action ID "
                            + actionBean.getId());
            // System.out.println("No Notification URL is defined. Therefore nothing is notified");
        }
        log.info("ENDED Coordinator Notification actionId="
                + actionBean.getId());
        return null;
    }

    private void handleRetry(String url) {
        if (retries < 3) {
            retries++;
            queueCallable(this, 60 * 1000);
        }
        else {
            XLog.getLog(getClass()).warn(XLog.OPS,
                                         "could not send notification [{0}]", url);
        }
    }

}
