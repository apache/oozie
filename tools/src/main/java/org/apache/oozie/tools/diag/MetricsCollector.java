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

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

class MetricsCollector {
    private final OozieClient client;

    MetricsCollector(OozieClient client) {
        this.client = client;
    }

    void storeMetrics(final File outputDir) {
        try {
            System.out.print("Getting Metrics...");
            final OozieClient.Metrics metrics = client.getMetrics();

            if (metrics == null) {
                System.out.println("Skipping (Metrics are unavailable)");
                return;
            }

            try (DiagBundleEntryWriter diagEntryWriter = new DiagBundleEntryWriter(outputDir, "metrics.txt")) {
                diagEntryWriter.writeString("COUNTERS\n")
                                 .writeString("--------\n");

                final Map<String, Long> counters = new TreeMap<>(metrics.getCounters());
                for (Map.Entry<String, Long> ent : counters.entrySet()) {
                    diagEntryWriter.writeLongValue(ent.getKey() + " : ", ent.getValue()).flush();
                }
                diagEntryWriter.writeNewLine()
                               .writeString("GAUGES\n")
                               .writeString("------\n");

                final Map<String, Object> gauges = new TreeMap<>(metrics.getGauges());
                for (Map.Entry<String, Object> ent : gauges.entrySet()) {
                    diagEntryWriter.writeStringValue(ent.getKey() + " : ", ent.getValue().toString());
                }
                diagEntryWriter.writeNewLine()
                               .writeString("TIMERS\n")
                               .writeString("------\n");

                final Map<String, OozieClient.Metrics.Timer> timers = new TreeMap<>(metrics.getTimers());
                for (Map.Entry<String, OozieClient.Metrics.Timer> ent : timers.entrySet()) {
                    diagEntryWriter.writeString(ent.getKey())
                                     .writeNewLine()
                                     .writeString(ent.getValue().toString())
                                     .writeNewLine();
                }
                diagEntryWriter.writeNewLine()
                               .writeString("HISTOGRAMS\n")
                               .writeString("----------\n");
                final Map<String, OozieClient.Metrics.Histogram> histograms =
                        new TreeMap<>(metrics.getHistograms());
                for (Map.Entry<String, OozieClient.Metrics.Histogram> ent : histograms.entrySet()) {
                    diagEntryWriter.writeString(ent.getKey())
                                     .writeNewLine()
                                     .writeString(ent.getValue().toString())
                                     .writeNewLine();
                }
                System.out.println("Done");
            }
        } catch (OozieClientException | IOException e) {
            System.err.printf("Exception occurred during the retrieval of Oozie metrics information: %s%n", e.getMessage());
        }
    }

    void storeInstrumentationInfo(final File outputDir) {
        try {
            System.out.print("Getting Instrumentation...");
            final OozieClient.Instrumentation instrumentation = client.getInstrumentation();
            if (instrumentation == null) {
                System.out.println("Skipping (Instrumentation is unavailable)");
                return;
            }

            try (DiagBundleEntryWriter diagEntryWriter = new DiagBundleEntryWriter(outputDir, "instrumentation.txt")) {
                diagEntryWriter.writeString("COUNTERS\n");
                diagEntryWriter.writeString("--------\n");

                final Map<String, Long> counters = new TreeMap<>(instrumentation.getCounters());
                for (Map.Entry<String, Long> ent : counters.entrySet()) {
                    diagEntryWriter.writeLongValue(ent.getKey() + " : ", ent.getValue()).flush();
                }
                diagEntryWriter.writeNewLine();
                diagEntryWriter.writeString("VARIABLES\n");
                diagEntryWriter.writeString("---------\n");

                final Map<String, Object> variables = new TreeMap<>(instrumentation.getVariables());
                for (Map.Entry<String, Object> ent : variables.entrySet()) {
                    diagEntryWriter.writeStringValue(ent.getKey() + " : ", ent.getValue().toString());
                }
                diagEntryWriter.writeNewLine();
                diagEntryWriter.writeString("SAMPLERS\n");
                diagEntryWriter.writeString("---------\n");

                final Map<String, Double> samplers = new TreeMap<>(instrumentation.getSamplers());
                for (Map.Entry<String, Double> ent : samplers.entrySet()) {
                    diagEntryWriter.writeStringValue(ent.getKey() + " : ", ent.getValue().toString());
                }
                diagEntryWriter.writeNewLine();
                diagEntryWriter.writeString("TIMERS\n");
                diagEntryWriter.writeString("---------\n");

                final Map<String, OozieClient.Instrumentation.Timer> timers =
                        new TreeMap<>(instrumentation.getTimers());
                for (Map.Entry<String, OozieClient.Instrumentation.Timer> ent : timers.entrySet()) {
                    diagEntryWriter.writeString(ent.getKey());
                    diagEntryWriter.writeNewLine();
                    diagEntryWriter.writeString(ent.getValue().toString());
                    diagEntryWriter.writeNewLine();
                }
                System.out.println("Done");
            }
        } catch (OozieClientException | IOException e) {
            System.err.printf("Exception occurred during the retrieval of Oozie instrumentation information: %s%n", e.getMessage());
        }
    }
}
