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
package org.apache.oozie;

import java.util.Properties;

/**
 * This class Provide Oozie build information.
 */
public class BuildInfo {

    public final static String BUILD_USER_NAME = "build.user";

    public final static String BUILD_TIME = "build.time";

    public final static String BUILD_VERSION = "build.version";

    public final static String BUILD_VC_REVISION = "vc.revision";

    public final static String BUILD_VC_URL = "vc.url";

    private static final Properties BUILD_INFO;

    static {
        BUILD_INFO = new Properties();
        try {
            BUILD_INFO.load(BuildInfo.class.getClassLoader().getResourceAsStream("oozie-buildinfo.properties"));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private BuildInfo() {
    }

    /**
     * Return the build info properties.
     *
     * @return the build info properties.
     */
    public static Properties getBuildInfo() {
        return BUILD_INFO;
    }

}
