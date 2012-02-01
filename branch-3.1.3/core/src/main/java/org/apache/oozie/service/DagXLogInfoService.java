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

import org.apache.oozie.util.XLogStreamer;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

/**
 * Set Dag specific log info parameters.
 */
public class DagXLogInfoService implements Service {

    /**
     * Token log info constant.
     */
    public static final String TOKEN = "TOKEN";

    /**
     * App log info constant.
     */
    public static final String APP = "APP";

    /**
     * Job log info constant.
     */
    public static final String JOB = "JOB";

    /**
     * Action log info constant.
     */
    public static final String ACTION = "ACTION";

    /**
     * Initialize the service.
     *
     * @param services services.
     */
    public void init(Services services) {
        XLog.Info.defineParameter(TOKEN);
        XLog.Info.defineParameter(APP);
        XLog.Info.defineParameter(JOB);
        XLog.Info.defineParameter(ACTION);

        XLogStreamer.Filter.defineParameter(TOKEN);
        XLogStreamer.Filter.defineParameter(APP);
        XLogStreamer.Filter.defineParameter(JOB);
        XLogStreamer.Filter.defineParameter(ACTION);

    }

    /**
     * Destroy the service.
     */
    public void destroy() {
    }

    /**
     * Return the public interface of the service.
     *
     * @return {@link DagXLogInfoService}.
     */
    public Class<? extends Service> getInterface() {
        return DagXLogInfoService.class;
    }

}
