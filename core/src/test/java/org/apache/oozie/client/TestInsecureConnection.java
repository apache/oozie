/*
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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.oozie.cli.CLIParser;
import org.apache.oozie.cli.OozieCLI;
import org.apache.oozie.cli.OozieCLIException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import javax.net.ssl.HttpsURLConnection;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;

@RunWith(Parameterized.class)
public class TestInsecureConnection {
    private static final String OOZIE_URL = "https://foobar"; // fake Oozie https URL

    private String[] oozieArgs;

    private MockedStatic<InsecureConnectionHelper> insecureConnectionHelperMockedStatic;

    // `createXOozieClient` is exposed in order to be able to create the `XOozieClient` which then creates the
    // AuthOozieClient.
    static class MyOozieCLI extends OozieCLI {
        @Override
        public XOozieClient createXOozieClient(CommandLine commandLine) throws OozieCLIException {
            return super.createXOozieClient(commandLine);
        }
    }

    MyOozieCLI myOozieCLI = new MyOozieCLI();
    final CLIParser parser = myOozieCLI.getCLIParser();

    private static final String[] adminVersionWithInsecureOption = new String[]{
            "admin",
            "-version",
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", "",
            "-insecure"
    };

    private static final String[] adminVersionWithoutInsecureOption = new String[]{
            "admin",
            "-version",
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", ""
    };

    private static final String[] jobsWithInsecureOption = new String[]{
            "jobs",
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", "",
            "-insecure"
    };

    private static final String[] jobsWithoutInsecureOption = new String[]{
            "jobs",
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", "",
            "-insecure"
    };

    private static final String[] jobKillWithInsecureOption = new String[]{
            "job",
            "-kill", "foobar",
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", "",
            "-insecure"
    };

    private static final String[] jobKillWithoutInsecureOption = new String[]{
            "job",
            "-kill", "foobar",
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", ""
    };

    private static final String[] slaFilterWithInsecureOption = new String[]{
            "sla",
            "-filter", "foobar",
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", "",
            "-insecure"
    };

    private static final String[] slaFilterWithoutInsecureOption = new String[]{
            "sla",
            "-filter", "foobar",
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", ""
    };

    private static final String[] pigWithInsecureOption = new String[]{
            "pig",
            "-config", createPropertiesFile(),
            "-file", createPigScript(),
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", "",
            "-insecure"
    };

    private static final String[] pigWithoutInsecureOption = new String[]{
            "pig",
            "-config", createPropertiesFile(),
            "-file", createPigScript(),
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", ""
    };

    private static final String[] hiveWithInsecureOption = new String[]{
            "hive",
            "-config", createPropertiesFile(),
            "-file", createHiveScript(),
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", "",
            "-insecure"
    };

    private static final String[] hiveWithoutInsecureOption = new String[]{
            "hive",
            "-config", createPropertiesFile(),
            "-file", createHiveScript(),
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", ""
    };

    private static final String[] sqoopWithInsecureOption = new String[]{
            "sqoop",
            "-config", createPropertiesFile(),
            "-command", "foobar",
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", "",
            "-insecure"
    };

    private static final String[] sqoopWithoutInsecureOption = new String[]{
            "sqoop",
            "-config", createPropertiesFile(),
            "-command", "foobar",
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", ""
    };

    private static final String[] mapreduceWithInsecureOption = new String[]{
            "mapreduce",
            "-config", createPropertiesFile(),
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", "",
            "-insecure"
    };

    private static final String[] mapreduceWithoutInsecureOption = new String[]{
            "mapreduce",
            "-config", createPropertiesFile(),
            "-oozie", OOZIE_URL,
            "-username", "sa",
            "-password", ""
    };

    private static final String[] validateWithInsecureOption = new String[]{
            "validate",
            "-username", "sa",
            "-password", "",
            "-oozie", OOZIE_URL,
            "-insecure",
            createDummyWorkflowFile()
    };

    private static final String[] validateWithoutInsecureOption = new String[]{
            "validate",
            "-username", "sa",
            "-password", "",
            "-oozie", OOZIE_URL,
            createDummyWorkflowFile()
    };

    public TestInsecureConnection(String testName, String[] oozieArgs) throws Exception {
        this.oozieArgs = oozieArgs;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection testCases() {
        return Arrays.asList(new Object[][]{
                {"Oozie admin version with -insecure option", adminVersionWithInsecureOption},
                {"Oozie admin version without -insecure option", adminVersionWithoutInsecureOption},
                {"Oozie jobs with -insecure option", jobsWithInsecureOption},
                {"Oozie jobs without -insecure option", jobsWithoutInsecureOption},
                {"Oozie job kill with -insecure option", jobKillWithInsecureOption},
                {"Oozie job kill without -insecure option", jobKillWithoutInsecureOption},
                {"Oozie sla filter with -insecure option", slaFilterWithInsecureOption},
                {"Oozie sla filter without -insecure option", slaFilterWithoutInsecureOption},
                {"Oozie pig with -insecure option", pigWithInsecureOption},
                {"Oozie pig without -insecure option", pigWithoutInsecureOption},
                {"Oozie hive with -insecure option", hiveWithInsecureOption},
                {"Oozie hive without -insecure option", hiveWithoutInsecureOption},
                {"Oozie sqoop with -insecure option", sqoopWithInsecureOption},
                {"Oozie sqoop without -insecure option", sqoopWithoutInsecureOption},
                {"Oozie mapreduce with -insecure option", mapreduceWithInsecureOption},
                {"Oozie mapreduce without -insecure option", mapreduceWithoutInsecureOption},
                {"Oozie validate with -insecure option", validateWithInsecureOption},
                {"Oozie validate without -insecure option", validateWithoutInsecureOption}
        });
    }

    @Before
    public void initTest() {
        insecureConnectionHelperMockedStatic = Mockito.mockStatic(InsecureConnectionHelper.class);
    }

    @After
    public void teardownTest() {
        insecureConnectionHelperMockedStatic.close();
    }

    @Test
    public void test() throws ParseException, OozieCLIException, IOException, OozieClientException {
        // creating the Oozie client based on the parameterized Oozie arguments
        XOozieClient createdOozieClient = myOozieCLI.createXOozieClient(parser.parse(oozieArgs).getCommandLine());

        // invoking the `createConnection` which is responsible for configuring the insecure connection in case
        // -insecure argument is being provided
        createdOozieClient.createConnection(new URL(OOZIE_URL), "POST");

        if (Arrays.asList(oozieArgs).contains("-insecure")) {
            insecureConnectionHelperMockedStatic.verify(() ->
                    InsecureConnectionHelper.configureInsecureAuthenticator(any(Authenticator.class)));
            insecureConnectionHelperMockedStatic.verify(() ->
                    InsecureConnectionHelper.configureInsecureConnection(any(HttpsURLConnection.class)));
        } else {
            insecureConnectionHelperMockedStatic.verifyNoInteractions();
        }
    }

    private static String createPropertiesFile() {
        String path = "tmp.properties";
        Properties props = new Properties();
        props.setProperty(OozieClient.USER_NAME, "sa");
        props.setProperty(XOozieClient.NN, "localhost:8020");
        props.setProperty(XOozieClient.RM, "localhost:8032");
        props.setProperty("oozie.libpath", "/tmp");
        props.setProperty("mapred.input.dir", "/tmp");
        props.setProperty("mapred.output.dir", "/tmp");
        props.setProperty("mapred.mapper.class", "foobar");
        props.setProperty("mapred.reducer.class", "foobar");
        props.setProperty("a", "A");

        try (OutputStream os = new FileOutputStream(path)) {
            props.store(os, "");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return path;
    }

    private static String createPigScript() {
        String path = "pig.script";
        String pigScript = "A = load '/user/data' using PigStorage(:);\n" +
                "B = foreach A generate $0" +
                "dumb B;";

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(path))) {
            dos.writeBytes(pigScript);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return path;
    }

    private static String createHiveScript() {
        String path = "hive.script";
        String hiveScript = "show tables;";

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(path))) {
            dos.writeBytes(hiveScript);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return path;
    }

    private static String createDummyWorkflowFile() {
        String path = "workflow.xml";

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(path))) {
            dos.writeBytes("");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return path;
    }
}