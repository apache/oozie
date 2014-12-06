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

package org.apache.oozie.event.listener;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ZKUtils;

/**
 * ZKConnectionListener listens on ZK connection status.
 */
public class ZKConnectionListener implements ConnectionStateListener {

    private XLog LOG = XLog.getLog(getClass());
    private static ConnectionState connectionState;
    public static final String CONF_SHUTDOWN_ON_TIMEOUT = "oozie.zookeeper.server.shutdown.ontimeout";

    public ZKConnectionListener() {
        LOG.info("ZKConnectionListener started");
    }

    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
        connectionState = newState;
        LOG.trace("ZK connection status  = " + newState.toString());
        // if (newState == ConnectionState.CONNECTED) {
        // ZK connected
        // }
        if (newState == ConnectionState.SUSPENDED) {
            LOG.warn("ZK connection is suspended, waiting for reconnect. If connection doesn't reconnect before "
                    + ZKUtils.getZKConnectionTimeout() + " (sec) Oozie server will shutdown itself");
        }

        if (newState == ConnectionState.RECONNECTED) {
            // ZK connected is reconnected.
            LOG.warn("ZK connection is reestablished");
        }

        if (newState == ConnectionState.LOST) {
            LOG.fatal("ZK is not reconnected in " + ZKUtils.getZKConnectionTimeout());
            if (ConfigurationService.getBoolean(CONF_SHUTDOWN_ON_TIMEOUT)) {
                LOG.fatal("Shutting down Oozie server");
                Services.get().destroy();
                System.exit(1);
            }
        }
    }

    public static ConnectionState getZKConnectionState() {
        return connectionState;
    }
}
