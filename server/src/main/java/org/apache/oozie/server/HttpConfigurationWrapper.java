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
package org.apache.oozie.server;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.server.HttpConfiguration;

/**
 *  Class that wraps HTTP configuration settings.
 */
public class HttpConfigurationWrapper {
    public static final String OOZIE_HTTP_REQUEST_HEADER_SIZE = "oozie.http.request.header.size";
    public static final String OOZIE_HTTP_RESPONSE_HEADER_SIZE = "oozie.http.response.header.size";
    private Configuration conf;

    public HttpConfigurationWrapper(Configuration conf) {
        this.conf = Preconditions.checkNotNull(conf, "conf");
    }

    /**
     * Set up and return default HTTP configuration for the Oozie server
     * @return default HttpConfiguration with the configured request and response header size
     */
    public HttpConfiguration getDefaultHttpConfiguration() {
        HttpConfiguration httpConfiguration = new HttpConfiguration();
        httpConfiguration.setRequestHeaderSize(
                getConfigHeaderSize(OOZIE_HTTP_REQUEST_HEADER_SIZE));
        httpConfiguration.setResponseHeaderSize(
                getConfigHeaderSize(OOZIE_HTTP_RESPONSE_HEADER_SIZE));
        httpConfiguration.setSendServerVersion(false);
        httpConfiguration.setSendXPoweredBy(false);
        return httpConfiguration;
    }

    private int getConfigHeaderSize(String confVar) {
        String confHeaderSize = conf.get(confVar);
        int headerSize;
        try {
            headerSize = Integer.parseInt(confHeaderSize);
        }
        catch (final NumberFormatException nfe) {
            throw new NumberFormatException(String.format("Header size for %s \"%s\" ( '%s') is not an integer.",
                    confVar, confVar, confHeaderSize));
        }
        return headerSize;
    }
}
