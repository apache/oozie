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

package org.apache.oozie.action.hadoop;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class LauncherURIHandlerFactory {

    public static final String CONF_LAUNCHER_URIHANDLER_SCHEME_PREFIX = "oozie.launcher.action.urihandler.scheme.";
    private Configuration conf;

    public LauncherURIHandlerFactory(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Get LauncherURIHandler to perform operations on a URI in the launcher
     * @param uri
     * @return LauncherURIHandler to perform operations on the URI
     * @throws LauncherException
     */
    public LauncherURIHandler getURIHandler(URI uri) throws LauncherException {
        LauncherURIHandler handler;
        if (uri.getScheme() == null) {
            handler = new FSLauncherURIHandler();
        }
        else {
            String className = conf.get(CONF_LAUNCHER_URIHANDLER_SCHEME_PREFIX + uri.getScheme());
            if (className == null) {
                className = conf.get(CONF_LAUNCHER_URIHANDLER_SCHEME_PREFIX + "*");
            }
            if (className == null) {
                throw new LauncherException("Scheme " + uri.getScheme() + " not supported in uri " + uri.toString());
            }
            Class<?> clazz;
            try {
                clazz = Class.forName(className);
            }
            catch (ClassNotFoundException e) {
                throw new LauncherException("Error instantiating LauncherURIHandler", e);
            }
            handler =  (LauncherURIHandler) ReflectionUtils.newInstance(clazz, null);
        }
        return handler;
    }

}
