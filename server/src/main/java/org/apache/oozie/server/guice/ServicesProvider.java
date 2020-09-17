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

package org.apache.oozie.server.guice;

import com.google.inject.Provider;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;

class ServicesProvider implements Provider<Services> {
    @Override
    public Services get() {
        Services oozieServices = null;
        try {
            oozieServices = new Services();
            oozieServices.init();

            return oozieServices;
        } catch (ServiceException e) {
            if (oozieServices != null) {
                oozieServices.destroy();
            }
            throw new IllegalStateException(
                    String.format("Could not instantiate Oozie services. [e.message=%s]", e.getMessage()));
        }
    }
}
