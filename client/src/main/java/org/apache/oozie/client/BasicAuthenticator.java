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

package org.apache.oozie.client;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BasicAuthenticator} is an empty implementation of the {@link Authenticator} interface,
 * because for Basic authentication, no need for a preemptive http request to gather authentication tokens.
 */
public class BasicAuthenticator implements Authenticator {
    private static final Logger LOG =
            LoggerFactory.getLogger(BasicAuthenticator.class);

    public BasicAuthenticator() {
    }

    /**
     * Sets a {@link ConnectionConfigurator} instance to use for
     * configuring connections.
     *
     * @param configurator the {@link ConnectionConfigurator} instance.
     */
    @Override
    public void setConnectionConfigurator(ConnectionConfigurator configurator) {
    }

    @Override
    public void authenticate(URL url, Token token) throws IOException, AuthenticationException {
        LOG.debug("Pretend authentication for {} with token {}", url, token);
    }

}
