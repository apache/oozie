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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.NotificationXCommand;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;

/**
 * This class will send the notification for the coordinator action
 */
public class CoordActionNotificationXCommand extends NotificationXCommand {

    private final CoordinatorActionBean actionBean;
    private static final String STATUS_PATTERN = "\\$status";
    private static final String ACTION_ID_PATTERN = "\\$actionId";

    // this variable is package private only for test purposes
    int retries = 0;

    public CoordActionNotificationXCommand(CoordinatorActionBean actionBean) {
        super("coord_action_notification", "coord_action_notification", 0);
        ParamChecker.notNull(actionBean, "Action Bean");
        this.actionBean = actionBean;
        jobId = actionBean.getId();

    }

    @Override
    protected void loadState() throws CommandException {
        Configuration conf;
        try {
            conf = new XConfiguration(new StringReader(actionBean.getRunConf()));
        }
        catch (IOException e1) {
            LOG.warn("Configuration parse error. read from DB :" + actionBean.getRunConf());
            throw new CommandException(ErrorCode.E1005, e1.getMessage(), e1);
        }
        url = conf.get(OozieClient.COORD_ACTION_NOTIFICATION_URL);
        if (url != null) {
            url = url.replaceAll(ACTION_ID_PATTERN, actionBean.getId());
            url = url.replaceAll(STATUS_PATTERN, actionBean.getStatus().toString());
            proxyConf = conf.get(OozieClient.COORD_ACTION_NOTIFICATION_PROXY,
                    Services.get().getConf().get(NOTIFICATION_PROXY_KEY));
            LOG.debug("Proxy :" + proxyConf);

        }
        LOG.debug("Notification URL :" + url);
        LogUtils.setLogInfo(actionBean);
    }
}
