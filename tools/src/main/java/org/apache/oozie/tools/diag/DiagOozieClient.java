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

package org.apache.oozie.tools.diag;

import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

class DiagOozieClient extends AuthOozieClient {
    DiagOozieClient(String oozieUrl, String authOption) {
        super(oozieUrl, authOption);
    }

    // Oozie has a jsp page with a thread dump, and the Oozie client normally doesn't have a way of getting it,
    // so we're doing that here; reusing all the fancy HTTP handling code in AuthOozieClient
    void saveThreadDumpPage(File file) throws OozieClientException, IOException {
        final URL url = new URL(super.getOozieUrl() + "admin/jvminfo.jsp");
        final HttpURLConnection retryableConnection = createRetryableConnection(url, "GET");

        if ((retryableConnection.getResponseCode() == HttpURLConnection.HTTP_OK)) {
            try (InputStream is = retryableConnection.getInputStream();
            final FileOutputStream os = new FileOutputStream(file)) {
                IOUtils.copyStream(is, os);
            }
        } else {
            throw new OozieClientException("HTTP error", retryableConnection.getResponseMessage());
        }
    }
}
