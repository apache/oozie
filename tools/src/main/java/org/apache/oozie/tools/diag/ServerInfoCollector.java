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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.OozieClientException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

class ServerInfoCollector {

    private final DiagOozieClient client;

    ServerInfoCollector(final DiagOozieClient client) {
        this.client = client;
    }

    void storeShareLibInfo(final File outputDir) {
        try {
            System.out.print("Getting Sharelib Information...");
            final String[] libs = client.listShareLib(null).split("\n");

            try (DiagBundleEntryWriter configEntryWriter = new DiagBundleEntryWriter(outputDir, "sharelib.txt")) {

                // Skip i=0 because it's always "[Available Sharelib]"
                for (int i = 1; i < libs.length; i++) {
                    String files = client.listShareLib(libs[i]);
                    configEntryWriter.writeString(files);
                    configEntryWriter.writeNewLine();
                }
            }
            System.out.println("Done");
        } catch (OozieClientException | IOException e) {
            System.err.printf("Exception occurred during the retrieval of ShareLib information: %s%n", e.getMessage());
        }
    }

    void storeJavaSystemProperties(final File outputDir) {
        try {
            System.out.print("Getting Java System Properties...");
            final Map<String, String> javaSysProps = client.getJavaSystemProperties();

            try (DiagBundleEntryWriter configEntryWriter = new DiagBundleEntryWriter(outputDir, "java-sys-props.txt")) {
                for (Map.Entry<String, String> ent : javaSysProps.entrySet()) {
                    configEntryWriter.writeStringValue(ent.getKey() + " : ", ent.getValue());
                }
            }
            System.out.println("Done");
        } catch (OozieClientException | IOException e) {
            System.err.printf("Exception occurred during the retrieval of Java system property settings for the Oozie server:" +
                    " %s%n", e.getMessage());
        }
    }

    void storeOsEnv(final File outputDir) {
        try {
            System.out.print("Getting OS Environment Variables...");
            final Map<String, String> osEnv = client.getOSEnv();

            try (DiagBundleEntryWriter configEntryWriter = new DiagBundleEntryWriter(outputDir,"os-env-vars.txt")) {
                for (Map.Entry<String, String> ent : osEnv.entrySet()) {
                    configEntryWriter.writeStringValue(ent.getKey() + " : ", ent.getValue());
                }
            }

            System.out.println("Done");
        } catch (OozieClientException | IOException e) {
            System.err.printf("Exception occurred during the retrieval of environment variable settings for the Oozie server:%s%n",
                    e.getMessage());
        }
    }

    void storeServerConfiguration(File outputDir) {
        try {
            System.out.print("Getting Configuration...");
            final Map<String, String> serverConfigMap = client.getServerConfiguration();
            final Configuration serverConfig = new Configuration(false);
            for (Map.Entry<String, String> ent : serverConfigMap.entrySet()) {
                serverConfig.set(ent.getKey(), ent.getValue());
            }

            try (OutputStream outputStream = new FileOutputStream(
                    new File(outputDir,  "effective-oozie-site.xml"))) {
                serverConfig.writeXml(outputStream);
            }
            System.out.println("Done");
        } catch (OozieClientException | IOException e) {
            System.err.printf("Exception occurred during the retrieval of effective Oozie server configuration " +
                    "\"oozie-site.xml\": %s%n", e.getMessage());
        }
    }

    void storeThreadDump(final File outputDir) {
        try {
            System.out.print("Getting Thread Dump...");
            client.saveThreadDumpPage(new File(outputDir, "thread-dump.html"));
            System.out.println("Done");
        } catch (OozieClientException | IOException e) {
            System.err.printf("Exception occurred during the retrieval of Oozie server thread dump: %s%n", e.getMessage());
        }
    }

    void storeCallableQueueDump(final File outputDir) {
        try {
            System.out.print("Getting Queue Dump...");
            final List<String> queueDump = client.getQueueDump();

            try (DiagBundleEntryWriter configEntryWriter = new DiagBundleEntryWriter(outputDir, "queue-dump.txt")) {
                for (String d : queueDump) {
                    configEntryWriter.writeString(d);
                    configEntryWriter.writeNewLine();
                }
            }

            System.out.println("Done");
        } catch (OozieClientException | IOException e) {
            System.err.println("Exception occurred during the retrieval of Oozie queue dump: " + e.getMessage());
        }
    }
}
