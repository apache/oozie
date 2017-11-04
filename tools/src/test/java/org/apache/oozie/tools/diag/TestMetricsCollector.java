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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class TestMetricsCollector {
    final DiagOozieClient client = new DiagOozieClient("testClient", null);
    private File testOut;

    @Rule public TemporaryFolder folder= new TemporaryFolder();
    @Mock private OozieClient mockOozieClient;

    @Before
    public void setup() throws IOException {
        testOut = folder.newFolder();
    }

    @Test
    public void testMetricsCanBeRetrieved() throws OozieClientException, IOException {
        String exampleMetrics = "{\n" +
                "   \"gauges\" : {\n" +
                "        \"jvm.memory.non-heap.committed\" : {\n" +
                "          \"value\" : 62590976\n" +
                "        },\n" +
                "        \"oozie.mode\" : {\n" +
                "          \"value\" : \"NORMAL\"\n" +
                "        }\n" +
                "    },\n" +
                "    \"timers\" : {\n" +
                "        \"commands.action.end.call.timer\" : {\n" +
                "          \"mean\" : 108.5,\n" +
                "          \"p50\" : 111.5,\n" +
                "          \"p75\" : 165.5,\n" +
                "          \"p999\" : 169,\n" +
                "          \"count\" : 4,\n" +
                "          \"p95\" : 169,\n" +
                "          \"max\" : 169,\n" +
                "          \"mean_rate\" : 0,\n" +
                "          \"duration_units\" : \"milliseconds\",\n" +
                "          \"p98\" : 169,\n" +
                "          \"m1_rate\" : 0,\n" +
                "          \"rate_units\" : \"calls/millisecond\",\n" +
                "          \"m15_rate\" : 0,\n" +
                "          \"stddev\" : 62.9417720330995,\n" +
                "          \"m5_rate\" : 0,\n" +
                "          \"p99\" : 169,\n" +
                "          \"min\" : 42\n" +
                "        }\n" +
                "    },\n" +
                "    \"histograms\" : {\n" +
                "        \"callablequeue.threads.active.histogram\" : {\n" +
                "          \"p999\" : 1,\n" +
                "          \"mean\" : 0.0625,\n" +
                "          \"min\" : 0,\n" +
                "          \"p75\" : 0,\n" +
                "          \"p95\" : 1,\n" +
                "          \"count\" : 48,\n" +
                "          \"p98\" : 1,\n" +
                "          \"stddev\" : 0.24462302739504083,\n" +
                "          \"max\" : 1,\n" +
                "          \"p99\" : 1,\n" +
                "          \"p50\" : 0\n" +
                "        },\n" +
                "    },\n" +
                "    \"counters\" : {\n" +
                "        \"commands.job.info.executions\" : {\n" +
                "          \"count\" : 9\n" +
                "        },\n" +
                "        \"jpa.CoordJobsGetPendingJPAExecutor\" : {\n" +
                "          \"count\" : 1\n" +
                "        },\n" +
                "        \"jpa.GET_WORKFLOW\" : {\n" +
                "          \"count\" : 10\n" +
                "        }\n" +
                "    }\n" +
                "}";
        final JSONObject testMetricsJSON = getJsonObject(exampleMetrics);

        final DiagOozieClient.Metrics metrics = client.new Metrics(testMetricsJSON);
        doReturn(metrics).when(mockOozieClient).getMetrics();
        final MetricsCollector metricsCollector = new MetricsCollector(mockOozieClient);

        metricsCollector.storeMetrics(testOut);

        final File metricsOut = new File(testOut, "metrics.txt");
        final String str = new String(Files.readAllBytes(metricsOut.toPath()), StandardCharsets.UTF_8.toString());

        assertTrue(str.contains("CoordJobsGetPendingJPAExecutor"));
    }

    @Test
    public void testInstrumentionCanBeRetrieved() throws OozieClientException, IOException {
        final String exampleInstrumentation = "{\n" +
                "  \"counters\": [\n" +
                "    {\n" +
                "      \"group\": \"db\",\n" +
                "      \"data\": [\n" +
                "        {\n" +
                "          \"name\": \"test\",\n" +
                "          \"value\": \"42\"\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"timers\": [\n" +
                "    {\n" +
                "      \"group\": \"db\",\n" +
                "      \"data\": [\n" +
                "        {\n" +
                "          \"ownMinTime\": 2,\n" +
                "          \"ownTimeStdDev\": 0,\n" +
                "          \"totalTimeStdDev\": 0,\n" +
                "          \"ownTimeAvg\": 3,\n" +
                "          \"ticks\": 117,\n" +
                "          \"name\": \"update-workflow\",\n" +
                "          \"ownMaxTime\": 32,\n" +
                "          \"totalMinTime\": 2,\n" +
                "          \"totalMaxTime\": 32,\n" +
                "          \"totalTimeAvg\": 3\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"samplers\": [\n" +
                "    {\n" +
                "      \"group\": \"callablequeue\",\n" +
                "      \"data\": [\n" +
                "        {\n" +
                "          \"name\": \"threads.active\",\n" +
                "          \"value\": 1.8333333333333333\n" +
                "        },\n" +
                "        {\n" +
                "          \"name\": \"delayed.queue.size\",\n" +
                "          \"value\": 0\n" +
                "        },\n" +
                "        {\n" +
                "          \"name\": \"queue.size\",\n" +
                "          \"value\": 0\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"variables\": [\n" +
                "    {\n" +
                "      \"group\": \"jvm\",\n" +
                "      \"data\": [\n" +
                "        {\n" +
                "          \"name\": \"max.memory\",\n" +
                "          \"value\": 506920960\n" +
                "        },\n" +
                "        {\n" +
                "          \"name\": \"total.memory\",\n" +
                "          \"value\": 56492032\n" +
                "        },\n" +
                "        {\n" +
                "          \"name\": \"free.memory\",\n" +
                "          \"value\": 45776800\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        final JSONObject testInstrumentationJSON = getJsonObject(exampleInstrumentation);
        final DiagOozieClient.Instrumentation metrics = client.new Instrumentation(testInstrumentationJSON);

        doReturn(metrics).when(mockOozieClient).getInstrumentation();
        final MetricsCollector metricsCollector = new MetricsCollector(mockOozieClient);

        metricsCollector.storeInstrumentationInfo(testOut);

        final File instrumentationFile = new File(testOut, "instrumentation.txt");
        assertTrue(instrumentationFile.exists());

        final String str = new String(Files.readAllBytes(instrumentationFile.toPath()), StandardCharsets.UTF_8.toString());
        assertTrue(str.contains("45776800"));
    }

    private JSONObject getJsonObject(final String exampleInstrumentation) {
        final JSONParser parser = new JSONParser();
        JSONObject testInstrumentationJSON = null;
        try {
            testInstrumentationJSON = (JSONObject) parser.parse(exampleInstrumentation);
        } catch (ParseException e) {
            fail();
        }
        return testInstrumentationJSON;
    }
}