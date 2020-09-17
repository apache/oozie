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

package org.apache.oozie.service;

import org.apache.oozie.service.SchemaService.SchemaName;
import org.apache.oozie.test.XTestCase;

import javax.xml.validation.Validator;
import javax.xml.transform.stream.StreamSource;

import java.io.StringReader;

public class TestSchemaService extends XTestCase {

    private static final String APP1 = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='app'>" +
            "  <start to='end'/>" +
            "  <end name='end'/>" +
            "</workflow-app>";

    private static final String APP_V2 = "<workflow-app xmlns='uri:oozie:workflow:0.2' name='app'>" + "<start to='end'/>"
            + "<end name='end'/>" + "</workflow-app>";

    private static final String APP_V25 = "<?xml version=\"1.0\"?>\n" +
            "<workflow-app xmlns=\"uri:oozie:workflow:0.2.5\" name=\"app\">\n" +
            "  <credentials/>\n" +
            "  <start to=\"end\"/>\n" +
            "  <end name=\"end\"/>\n" +
            "</workflow-app>";

    private static final String WF_SLA_APP = "<workflow-app xmlns='uri:oozie:workflow:0.2' name='app'"
            + "  xmlns:sla='uri:oozie:sla:0.1'>"
            + "  <start to='end'/>"
            + "  <end name='end'/>"
            + "  <sla:info><sla:app-name>5</sla:app-name> <sla:nominal-time>2009-03-06T010:00Z</sla:nominal-time> "
            + "  <sla:should-start>5</sla:should-start> <sla:should-end>50</sla:should-end> "
            + "<sla:alert-contact>abc@example.com</sla:alert-contact> <sla:dev-contact>abc@example.com</sla:dev-contact>"
            + " <sla:qa-contact>abc@example.com</sla:qa-contact> <sla:se-contact>abc@example.com</sla:se-contact>"
            + "</sla:info>" + "</workflow-app>";

    private static final String WF_SLA_APP_NW = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='app'" +
            " xmlns:sla='uri:oozie:sla:0.1'>"
            + "<start to='end'/>"
            + "<end name='end'/>"
            + "<sla:info> <sla:app-name>5</sla:app-name> <sla:nominal-time>2009-03-06T010:00Z</sla:nominal-time> "
            + "<sla:should-start>5</sla:should-start> <sla:should-end>50</sla:should-end> "
            + "<sla:alert-contact>abc@example.com</sla:alert-contact> <sla:dev-contact>abc@example.com</sla:dev-contact>"
            + " <sla:qa-contact>abc@example.com</sla:qa-contact> <sla:se-contact>abc@example.com</sla:se-contact>"
            + "</sla:info>" + "</workflow-app>";

    private static final String APP2 = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='app'>" +
            "<start to='a'/>" +
            "<action name='a'>" +
            "<test xmlns='uri:test'>" +
            "<signal-value>a</signal-value>" +
            "<external-status>b</external-status>" +
            "<error>c</error>" +
            "<avoid-set-execution-data>d</avoid-set-execution-data>" +
            "<avoid-set-end-data>d</avoid-set-end-data>" +
            "<running-mode>e</running-mode>" +
            "</test>" +
            "<ok to='end'/>" +
            "<error to='end'/>" +
            "</action>" +
            "<end name='end'/>" +
            "</workflow-app>";

    private static final String WF_4_MULTIPLE_JAVA_OPTS = "<workflow-app xmlns='uri:oozie:workflow:0.4' name ='app'>" +
            "<start to='a'/>" +
            "<action name='a'>" +
            "<java>" +
            "<job-tracker>JT</job-tracker>" +
            "<name-node>NN</name-node>" +
            "<main-class>main.java</main-class>" +
            "<java-opt>-Dparam1=1</java-opt>" +
            "<java-opt>-Dparam2=2</java-opt>" +
            "</java>" +
            "<ok to='end'/>" +
            "<error to='end'/>" +
            "</action>" +
             "<end name='end'/>" +
            "</workflow-app>";

    private static final String WF_GLOBAL_LAUNCHER_CONF = "<workflow-app xmlns=\"uri:oozie:workflow:1.0\" name=\"test-wf\">\n" +
            "    <global>\n" +
            "        <launcher>\n" +
            "            <memory.mb>1024</memory.mb>\n" +
            "            <vcores>2</vcores>\n" +
            "            <java-opts>dummyJavaOpts</java-opts>\n" +
            "            <env>dummyEnv</env>\n" +
            "            <queue>dummyQueue</queue>\n" +
            "            <sharelib>a,b,c</sharelib>\n" +
            "            <view-acl>oozie</view-acl>\n" +
            "            <modify-acl>oozie</modify-acl>\n" +
            "        </launcher>\n" +
            "    </global>\n" +
            "    <start to=\"a\"/>\n" +
            "    <action name=\"a\">\n" +
            "        <fs>\n" +
            "            <mkdir path='/tmp'/>\n" +
            "        </fs>\n" +
            "        <ok to=\"e\"/>\n" +
            "        <error to=\"k\"/>\n" +
            "    </action>\n" +
            "    <kill name=\"k\">\n" +
            "        <message>kill</message>\n" +
            "    </kill>\n" +
            "    <end name=\"e\"/>\n" +
            "</workflow-app>\n";

    private static final String HIVE_ACTION_LAUNCHER_CONF =
            "<workflow-app xmlns=\"uri:oozie:workflow:1.0\" name=\"hive-wf\">\n" +
                    "    <start to=\"hive-node\"/>\n" +
                    "    <action name=\"hive-node\">\n" +
                    "        <hive xmlns=\"uri:oozie:hive-action:1.0\">\n" +
                    "            <job-tracker>${jobTracker}</job-tracker>\n" +
                    "            <name-node>${nameNode}</name-node>\n" +
                    "            <prepare>\n" +
                    "                <delete path=\"${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/hive\"/>\n" +
                    "                <mkdir path=\"${nameNode}/user/${wf:user()}/${examplesRoot}/output-data\"/>\n" +
                    "            </prepare>\n" +
                    "            <launcher>\n" +
                    "                <memory.mb>1024</memory.mb>\n" +
                    "                <vcores>2</vcores>\n" +
                    "                <java-opts>dummyJavaOpts</java-opts>\n" +
                    "                <env>dummyEnv</env>\n" +
                    "                <queue>dummyQueue</queue>\n" +
                    "                <sharelib>a,b,c</sharelib>\n" +
                    "            </launcher>\n" +
                    "            <configuration>\n" +
                    "                <property>\n" +
                    "                    <name>mapred.job.queue.name</name>\n" +
                    "                    <value>${queueName}</value>\n" +
                    "                </property>\n" +
                    "            </configuration>\n" +
                    "            <script>script.q</script>\n" +
                    "            <param>INPUT=/user/${wf:user()}/${examplesRoot}/input-data/table</param>\n" +
                    "            <param>OUTPUT=/user/${wf:user()}/${examplesRoot}/output-data/hive</param>\n" +
                    "        </hive>\n" +
                    "        <ok to=\"end\"/>\n" +
                    "        <error to=\"fail\"/>\n" +
                    "    </action>\n" +
                    "    <kill name=\"fail\">\n" +
                    "        <message>Hive failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
                    "    </kill>\n" +
                    "    <end name=\"end\"/>\n" +
                    "</workflow-app>\n";

    private static final String HIVE2_ACTION_LAUNCHER_CONF =
            "<workflow-app xmlns=\"uri:oozie:workflow:1.0\" name=\"hive2-wf\">\n" +
            "    <start to=\"hive2-node\"/>\n" +
            "    <action name=\"hive2-node\">\n" +
            "        <hive2 xmlns=\"uri:oozie:hive2-action:1.0\">\n" +
            "            <job-tracker>${jobTracker}</job-tracker>\n" +
            "            <name-node>${nameNode}</name-node>\n" +
            "            <prepare>\n" +
            "                <delete path=\"${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/hive2\"/>\n" +
            "                <mkdir path=\"${nameNode}/user/${wf:user()}/${examplesRoot}/output-data\"/>\n" +
            "            </prepare>\n" +
            "            <launcher>\n" +
            "                <memory.mb>1024</memory.mb>\n" +
            "                <vcores>2</vcores>\n" +
            "                <java-opts>dummyJavaOpts</java-opts>\n" +
            "                <env>dummyEnv</env>\n" +
            "                <queue>dummyQueue</queue>\n" +
            "                <sharelib>a,b,c</sharelib>\n" +
            "            </launcher>\n" +
            "            <configuration>\n" +
            "                <property>\n" +
            "                    <name>mapred.job.queue.name</name>\n" +
            "                    <value>${queueName}</value>\n" +
            "                </property>\n" +
            "            </configuration>\n" +
            "            <jdbc-url>${jdbcURL}</jdbc-url>\n" +
            "            <script>script.q</script>\n" +
            "            <param>INPUT=/user/${wf:user()}/${examplesRoot}/input-data/table</param>\n" +
            "            <param>OUTPUT=/user/${wf:user()}/${examplesRoot}/output-data/hive2</param>\n" +
            "        </hive2>\n" +
            "        <ok to=\"end\"/>\n" +
            "        <error to=\"fail\"/>\n" +
            "    </action>\n" +
            "    <kill name=\"fail\">\n" +
            "        <message>Hive2 (Beeline) action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
            "    </kill>\n" +
            "    <end name=\"end\"/>\n" +
            "</workflow-app>\n";

    private static final String SHELL_ACTION_LAUNCHER_CONF = "<workflow-app xmlns=\"uri:oozie:workflow:1.0\" " +
            "    name=\"shell-wf\">\n" +
            "    <start to=\"shell-node\"/>\n" +
            "    <action name=\"shell-node\">\n" +
            "        <shell xmlns=\"uri:oozie:shell-action:1.0\">\n" +
            "            <job-tracker>${jobTracker}</job-tracker>\n" +
            "            <name-node>${nameNode}</name-node>\n" +
            "            <launcher>\n" +
            "                <memory.mb>1024</memory.mb>\n" +
            "            </launcher>\n" +
            "            <configuration>\n" +
            "                <property>\n" +
            "                    <name>mapred.job.queue.name</name>\n" +
            "                    <value>${queueName}</value>\n" +
            "                </property>\n" +
            "            </configuration>\n" +
            "            <exec>echo</exec>\n" +
            "            <argument>my_output=Hello Oozie</argument>\n" +
            "            <capture-output/>\n" +
            "        </shell>\n" +
            "        <ok to=\"check-output\"/>\n" +
            "        <error to=\"fail\"/>\n" +
            "    </action>\n" +
            "    <decision name=\"check-output\">\n" +
            "        <switch>\n" +
            "            <case to=\"end\">\n" +
            "                ${wf:actionData('shell-node')['my_output'] eq 'Hello Oozie'}\n" +
            "            </case>\n" +
            "            <default to=\"fail-output\"/>\n" +
            "        </switch>\n" +
            "    </decision>\n" +
            "    <kill name=\"fail\">\n" +
            "        <message>Shell action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
            "    </kill>\n" +
            "    <kill name=\"fail-output\">\n" +
            "        <message>Incorrect output</message>\n" +
            "    </kill>\n" +
            "    <end name=\"end\"/>\n" +
            "</workflow-app>";

    private static final String SQQP_ACTION_LAUNCHER_CONF = "<workflow-app xmlns=\"uri:oozie:workflow:1.0\" name=\"sqoop-wf\">\n" +
            "    <start to=\"sqoop-node\"/>\n" +
            "    <action name=\"sqoop-node\">\n" +
            "        <sqoop xmlns=\"uri:oozie:sqoop-action:1.0\">\n" +
            "            <job-tracker>${jobTracker}</job-tracker>\n" +
            "            <name-node>${nameNode}</name-node>\n" +
            "            <prepare>\n" +
            "                <delete path=\"${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/sqoop\"/>\n" +
            "                <mkdir path=\"${nameNode}/user/${wf:user()}/${examplesRoot}/output-data\"/>\n" +
            "            </prepare>\n" +
            "            <launcher>\n" +
            "                <memory.mb>1024</memory.mb>\n" +
            "            </launcher>\n" +
            "            <configuration>\n" +
            "                <property>\n" +
            "                    <name>mapred.job.queue.name</name>\n" +
            "                    <value>${queueName}</value>\n" +
            "                </property>\n" +
            "            </configuration>\n" +
            "            <command>import --connect jdbc:hsqldb:file:db.hsqldb --table TT --target-dir " +
            "/user/${wf:user()}/${examplesRoot}/output-data/sqoop -m 1</command>\n" +
            "            <file>db.hsqldb.properties#db.hsqldb.properties</file>\n" +
            "            <file>db.hsqldb.script#db.hsqldb.script</file>\n" +
            "        </sqoop>\n" +
            "        <ok to=\"end\"/>\n" +
            "        <error to=\"fail\"/>\n" +
            "    </action>\n" +
            "    <kill name=\"fail\">\n" +
            "        <message>Sqoop failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>\n" +
            "    </kill>\n" +
            "    <end name=\"end\"/>\n" +
            "</workflow-app>";

    private static final String SPARK_ACTION_LAUNCHER_CONF = "<workflow-app xmlns='uri:oozie:workflow:1.0' " +
            "name='SparkFileCopy'>\n" +
            "    <start to='spark-node' />\n" +
            "    <action name='spark-node'>\n" +
            "        <spark xmlns=\"uri:oozie:spark-action:1.0\">\n" +
            "            <job-tracker>${jobTracker}</job-tracker>\n" +
            "            <name-node>${nameNode}</name-node>\n" +
            "            <prepare>\n" +
            "                <delete path=\"${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/spark\"/>\n" +
            "            </prepare>\n" +
            "            <launcher>\n" +
            "                <memory.mb>1024</memory.mb>\n" +
            "                <vcores>2</vcores>\n" +
            "                <java-opts>dummyJavaOpts</java-opts>\n" +
            "                <env>dummyEnv</env>\n" +
            "                <queue>dummyQueue</queue>\n" +
            "                <sharelib>a,b,c</sharelib>\n" +
            "            </launcher>\n" +
            "            <master>${master}</master>\n" +
            "            <name>Spark-FileCopy</name>\n" +
            "            <class>org.apache.oozie.example.SparkFileCopy</class>\n" +
            "            <jar>${nameNode}/user/${wf:user()}/${examplesRoot}/apps/spark/lib/oozie-examples.jar</jar>\n" +
            "            <arg>${nameNode}/user/${wf:user()}/${examplesRoot}/input-data/text/data.txt</arg>\n" +
            "            <arg>${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/spark</arg>\n" +
            "        </spark>\n" +
            "        <ok to=\"end\" />\n" +
            "        <error to=\"fail\" />\n" +
            "    </action>\n" +
            "    <kill name=\"fail\">\n" +
            "        <message>Workflow failed, error\n" +
            "            message[${wf:errorMessage(wf:lastErrorNode())}]\n" +
            "        </message>\n" +
            "    </kill>\n" +
            "    <end name='end' />\n" +
            "</workflow-app>\n";

    private SchemaService wss;
    private Validator workflowValidator;
    private Validator coordinatorValidator;
    private Validator bundleValidator;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
        wss = Services.get().get(SchemaService.class);
        workflowValidator = wss.getValidator(SchemaName.WORKFLOW);
        coordinatorValidator = wss.getValidator(SchemaName.COORDINATOR);
        bundleValidator = wss.getValidator(SchemaName.BUNDLE);
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testService() throws Exception {
        assertNotNull(Services.get().get(SchemaService.class));
    }

    public void testWfSchema() throws Exception {
        workflowValidator.validate(new StreamSource(new StringReader(APP1)));
    }

    public void testWfMultipleJavaOpts() throws Exception {
        workflowValidator.validate(new StreamSource(new StringReader(WF_4_MULTIPLE_JAVA_OPTS)));
    }

    public void testWfSchemaV2() throws Exception {
        workflowValidator.validate(new StreamSource(new StringReader(APP_V2)));
    }

    public void testWfSchemaV25() throws Exception {
        workflowValidator.validate(new StreamSource(new StringReader(APP_V25)));
    }

    public void testExtSchema() throws Exception {
        Services.get().destroy();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        new Services().init();
        SchemaService wss = Services.get().get(SchemaService.class);
        Validator validator = wss.getValidator(SchemaName.WORKFLOW);
        validator.validate(new StreamSource(new StringReader(APP2)));
    }

    public void testWfSLASchema() throws Exception {
        workflowValidator.validate(new StreamSource(new StringReader(WF_SLA_APP)));
    }

    public void testWfSLASchemaNW() throws Exception {
        try {
            workflowValidator.validate(new StreamSource(new StringReader(WF_SLA_APP_NW)));
            fail("Schema service check does not work");
        }
        catch (Exception ex) {
            // Expected
        }
    }

    public void testCoordSchema() throws Exception {
        String COORD_APP1 = "<?xml version=\"1.0\"?>\n" +
                "<coordinator-app xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"uri:oozie:coordinator:0.1\"" +
                " xmlns:sla=\"uri:oozie:sla:0.1\" name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\"" +
                " end=\"2009-02-03T23:59Z\" timezone=\"UTC\">\n" +
                "  <controls>\n" +
                "    <timeout>10</timeout>\n" +
                "    <concurrency>2</concurrency>\n" +
                "    <execution>LIFO</execution>\n" +
                "  </controls>\n" +
                "  <datasets>\n" +
                "    <dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\"" +
                "        timezone=\"UTC\">\n" +
                "      <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>\n" +
                "    </dataset>\n" +
                "    <dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\"" +
                "        timezone=\"UTC\">\n" +
                "      <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>\n" +
                "    </dataset>\n" +
                "  </datasets>\n" +
                "  <input-events>\n" +
                "    <data-in name=\"A\" dataset=\"a\">\n" +
                "      <instance>${coord:latest(0)}</instance>\n" +
                "    </data-in>\n" +
                "  </input-events>\n" +
                "  <output-events>\n" +
                "    <data-out name=\"LOCAL_A\" dataset=\"local_a\">\n" +
                "      <instance>${coord:current(-1)}</instance>\n" +
                "    </data-out>\n" +
                "  </output-events>\n" +
                "  <action>\n" +
                "    <workflow>\n" +
                "      <app-path>hdfs:///tmp/workflows/</app-path>\n" +
                "      <configuration>\n" +
                "        <property>\n" +
                "          <name>inputA</name>\n" +
                "          <value>${coord:dataIn('A')}</value>\n" +
                "        </property>\n" +
                "        <property>\n" +
                "          <name>inputB</name>\n" +
                "          <value>${coord:dataOut('LOCAL_A')}</value>\n" +
                "        </property>\n" +
                "      </configuration>\n" +
                "    </workflow>\n" +
                "  </action>\n" +
                "</coordinator-app>";

        coordinatorValidator.validate(new StreamSource(new StringReader(COORD_APP1)));
    }

    public void testCoordSchema2() throws Exception {
        String COORD_APP1 = "<coordinator-app xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" +
                "    xmlns=\"uri:oozie:coordinator:0.2\" xmlns:sla=\"uri:oozie:sla:0.1\" name=\"NAME\"" +
                "    frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\" end=\"2009-02-03T23:59Z\" timezone=\"UTC\">\n" +
                "  <controls>\n" +
                "    <timeout>10</timeout>\n" +
                "    <concurrency>2</concurrency>\n" +
                "    <execution>LIFO</execution>\n" +
                "    <throttle>3</throttle>\n" +
                "  </controls>\n" +
                "  <datasets>\n" +
                "    <dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\"" +
                "        timezone=\"UTC\">\n" +
                "      <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>\n" +
                "    </dataset>\n" +
                "    <dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\"" +
                "        timezone=\"UTC\">\n" +
                "      <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>\n" +
                "    </dataset>\n" +
                "  </datasets>\n" +
                "  <input-events>\n" +
                "    <data-in name=\"A\" dataset=\"a\">\n" +
                "      <instance>${coord:latest(0)}</instance>\n" +
                "    </data-in>\n" +
                "  </input-events>\n" +
                "  <output-events>\n" +
                "    <data-out name=\"LOCAL_A\" dataset=\"local_a\">\n" +
                "      <instance>${coord:current(-1)}</instance>\n" +
                "    </data-out>\n" +
                "  </output-events>\n" +
                "  <action>\n" +
                "    <workflow>\n" +
                "      <app-path>hdfs:///tmp/workflows/</app-path>\n" +
                "      <configuration>\n" +
                "        <property>\n" +
                "          <name>inputA</name>\n" +
                "          <value>${coord:dataIn('A')}</value>\n" +
                "        </property>\n" +
                "        <property>\n" +
                "          <name>inputB</name>\n" +
                "          <value>${coord:dataOut('LOCAL_A')}</value>\n" +
                "        </property>\n" +
                "      </configuration>\n" +
                "    </workflow>\n" +
                "  </action>\n" +
                "</coordinator-app>";

        coordinatorValidator.validate(new StreamSource(new StringReader(COORD_APP1)));
    }

    public void testCoordSLASchema() throws Exception {
        String COORD_APP1 = "<?xml version=\"1.0\"?>\n" +
                "<coordinator-app xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"uri:oozie:coordinator:0.2\"" +
                "    xmlns:sla=\"uri:oozie:sla:0.1\" name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"2009-02-01T01:00Z\"" +
                "    end=\"2009-02-03T23:59Z\" timezone=\"UTC\">\n" +
                "  <controls>\n" +
                "    <timeout>10</timeout>\n" +
                "    <concurrency>2</concurrency>\n" +
                "    <execution>LIFO</execution>\n" +
                "  </controls>\n" +
                "  <datasets>\n" +
                "    <dataset name=\"a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\"" +
                "         timezone=\"UTC\">\n" +
                "      <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>\n" +
                "    </dataset>\n" +
                "    <dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"2009-02-01T01:00Z\"" +
                "         timezone=\"UTC\">\n" +
                "      <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>\n" +
                "    </dataset>\n" +
                "  </datasets>\n" +
                "  <input-events>\n" +
                "    <data-in name=\"A\" dataset=\"a\">\n" +
                "      <instance>${coord:latest(0)}</instance>\n" +
                "    </data-in>\n" +
                "  </input-events>\n" +
                "  <output-events>\n" +
                "    <data-out name=\"LOCAL_A\" dataset=\"local_a\">\n" +
                "      <instance>${coord:current(-1)}</instance>\n" +
                "    </data-out>\n" +
                "  </output-events>\n" +
                "  <action>\n" +
                "    <workflow>\n" +
                "      <app-path>hdfs:///tmp/workflows/</app-path>\n" +
                "      <configuration>\n" +
                "        <property>\n" +
                "          <name>inputA</name>\n" +
                "          <value>${coord:dataIn('A')}</value>\n" +
                "        </property>\n" +
                "        <property>\n" +
                "          <name>inputB</name>\n" +
                "          <value>${coord:dataOut('LOCAL_A')}</value>\n" +
                "        </property>\n" +
                "      </configuration>\n" +
                "    </workflow>\n" +
                "    <sla:info>\n" +
                "      <sla:app-name>5</sla:app-name>\n" +
                "      <sla:nominal-time>2009-03-06T010:00Z</sla:nominal-time>\n" +
                "      <sla:should-start>5</sla:should-start>\n" +
                "      <sla:should-end>50</sla:should-end>\n" +
                "      <sla:alert-contact>abc@example.com</sla:alert-contact>\n" +
                "      <sla:dev-contact>abc@example.com</sla:dev-contact>\n" +
                "      <sla:qa-contact>abc@example.com</sla:qa-contact>\n" +
                "      <sla:se-contact>abc@example.com</sla:se-contact>\n" +
                "    </sla:info>\n" +
                "  </action>\n" +
                "</coordinator-app>";

        coordinatorValidator.validate(new StreamSource(new StringReader(COORD_APP1)));
    }

    public void testBundleSchema() throws Exception {
        String BUNDLE_APP = "<bundle-app name='NAME' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' "
                + " xmlns='uri:oozie:bundle:0.1'> "
                + "<controls> <kick-off-time>2009-02-02T00:00Z</kick-off-time> </controls> "
                + "<coordinator name='c12'> "
                + "<app-path>hdfs://localhost:9001/tmp/bundle-apps/coordinator1.xml</app-path>"
                + "<configuration> "
                + "<property> <name>START_TIME</name> <value>2009-02-01T00:00Z</value> </property> </configuration> "
                + "</coordinator></bundle-app>";

        bundleValidator.validate(new StreamSource(new StringReader(BUNDLE_APP)));
    }

    public void testWfLauncherConfig() throws Exception {
        workflowValidator.validate(new StreamSource(new StringReader(WF_GLOBAL_LAUNCHER_CONF)));
    }

    public void testHiveActionLauncherConfig() throws Exception {
        workflowValidator.validate(new StreamSource(new StringReader(HIVE_ACTION_LAUNCHER_CONF)));
    }

    public void testHive2ActionLauncherConfig() throws Exception {
        workflowValidator.validate(new StreamSource(new StringReader(HIVE2_ACTION_LAUNCHER_CONF)));
    }

    public void testShellActionLauncherConfig() throws Exception {
        workflowValidator.validate(new StreamSource(new StringReader(SHELL_ACTION_LAUNCHER_CONF)));
    }

    public void testSqoopActionLauncherConfig() throws Exception {
        workflowValidator.validate(new StreamSource(new StringReader(SQQP_ACTION_LAUNCHER_CONF)));
    }

    public void testSparkActionLauncherConfig() throws Exception {
        workflowValidator.validate(new StreamSource(new StringReader(SPARK_ACTION_LAUNCHER_CONF)));
    }
}
