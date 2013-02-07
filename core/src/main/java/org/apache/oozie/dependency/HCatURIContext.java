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
package org.apache.oozie.dependency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hcatalog.api.HCatClient;
import org.apache.oozie.util.XLog;

public class HCatURIContext extends URIContext {

    private static XLog LOG = XLog.getLog(HCatURIContext.class);
    private HCatClient hcatClient;

    /**
     * Create a HCatURIContext that can be used to access a hcat URI
     *
     * @param conf Configuration to access the URI
     * @param user name of the user the URI should be accessed as
     * @param hcatClient HCatClient to talk to hcatalog server
     */
    public HCatURIContext(Configuration conf, String user, HCatClient hcatClient) {
        super(conf, user);
        this.hcatClient = hcatClient;
    }

    /**
     * Get the HCatClient to talk to hcatalog server
     *
     * @return HCatClient to talk to hcatalog server
     */
    public HCatClient getHCatClient() {
        return hcatClient;
    }

    @Override
    public void destroy() {
        try {
            hcatClient.close();
        }
        catch (Exception ignore) {
            LOG.warn("Error closing hcat client", ignore);
        }
    }

}
