/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.service;

import org.apache.oozie.service.Services;
import org.apache.oozie.service.SchemaService.SchemaName;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

import javax.xml.validation.Validator;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;

public class TestSchemaService extends XTestCase {


    private static final String APP1 = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='app'>" +
            "<start to='end'/>" +
            "<end name='end'/>" +
            "</workflow-app>";

    private static final String APP_V2 = "<workflow-app xmlns='uri:oozie:workflow:0.2' name='app'>" + "<start to='end'/>"
            + "<end name='end'/>" + "</workflow-app>";

    private static final String WF_SLA_APP = "<workflow-app xmlns='uri:oozie:workflow:0.2' name='app'  xmlns:sla='uri:oozie:sla:0.1'>"
            + "<start to='end'/>"
            + "<end name='end'/>"
            + "<sla:info> <sla:app-name>5</sla:app-name> <sla:nominal-time>2009-03-06T010:00Z</sla:nominal-time> "
            + "<sla:should-start>5</sla:should-start> <sla:should-end>50</sla:should-end> "
            + "<sla:alert-contact>abc@yahoo.com</sla:alert-contact> <sla:dev-contact>abc@yahoo.com</sla:dev-contact>"
            + " <sla:qa-contact>abc@yahoo.com</sla:qa-contact> <sla:se-contact>abc@yahoo.com</sla:se-contact>"
            + "</sla:info>" + "</workflow-app>";

    private static final String WF_SLA_APP_NW = "<workflow-app xmlns='uri:oozie:workflow:0.1' name='app'  xmlns:sla='uri:oozie:sla:0.1'>"
            + "<start to='end'/>"
            + "<end name='end'/>"
            + "<sla:info> <sla:app-name>5</sla:app-name> <sla:nominal-time>2009-03-06T010:00Z</sla:nominal-time> "
            + "<sla:should-start>5</sla:should-start> <sla:should-end>50</sla:should-end> "
            + "<sla:alert-contact>abc@yahoo.com</sla:alert-contact> <sla:dev-contact>abc@yahoo.com</sla:dev-contact>"
            + " <sla:qa-contact>abc@yahoo.com</sla:qa-contact> <sla:se-contact>abc@yahoo.com</sla:se-contact>"
            + "</sla:info>" + "</workflow-app>";

    private static final String COORD_APP1 = "<coordinator-app name=\"NAME\" frequency=\"${coord:days(1)}\" start=\"${start}\" end=\"${end}\" timezone=\"${timezone}\" xmlns=\"uri:oozie:coordinator:0.1\">"
            + "<controls> <timeout>${timeout}</timeout> <concurrency>${concurrency_level}</concurrency> <execution>${execution_order}</execution> </controls>"
            + "<datasets> <include>${include_ds_files}</include> <!-- Synchronous datasets --> <dataset name=\"local_a\" frequency=\"${coord:days(7)}\" initial-instance=\"${start}\" timezone=\"${timezone}\"> "
            + "<uri-template>${baseFsURI}/${YEAR}/${DAY}</uri-template> </dataset> </datasets> "
            + "<input-events> <data-in name=\"A\" dataset=\"a\"> <instance>${coord:latest(0)}</instance> </data-in> <data-in name=\"B\" dataset=\"b\"> <start-instance>${coord:current(-2)}</start-instance> <end-instance>${coord:current(0)}</end-instance> </data-in> </input-events> <output-events> "
            + "<data-out name=\"LOCAL_A\" dataset=\"local_a\"> <instance>${coord:current(0)}</instance> </data-out> </output-events> <action> <workflow> <app-path>${app_path}</app-path> <configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> <property> <name>inputB</name> <value>${coord:dataIn('B')}</value> </property> <property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> </property> <property> <name>TESTING</name> <value>${start}</value> </property> </configuration> </workflow> </action> </coordinator-app>";


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

    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testService() throws Exception {
        assertNotNull(Services.get().get(SchemaService.class));
    }

    public void testWfSchema() throws Exception {
        SchemaService wss = Services.get().get(SchemaService.class);
        Validator validator = wss.getSchema(SchemaName.WORKFLOW).newValidator();
        validator.validate(new StreamSource(new StringReader(APP1)));
    }

    public void testWfSchemaV2() throws Exception {
        SchemaService wss = Services.get().get(SchemaService.class);
        Validator validator = wss.getSchema(SchemaName.WORKFLOW).newValidator();
        validator.validate(new StreamSource(new StringReader(APP_V2)));
    }

    public void testExtSchema() throws Exception {
        Services.get().destroy();
        setSystemProperty(SchemaService.WF_CONF_EXT_SCHEMAS, "wf-ext-schema.xsd");
        new Services().init();
        SchemaService wss = Services.get().get(SchemaService.class);
        Validator validator = wss.getSchema(SchemaName.WORKFLOW).newValidator();
        validator.validate(new StreamSource(new StringReader(APP2)));
    }

    public void testWfSLASchema() throws Exception {
        SchemaService wss = Services.get().get(SchemaService.class);
        Validator validator = wss.getSchema(SchemaName.WORKFLOW).newValidator();
        validator.validate(new StreamSource(new StringReader(WF_SLA_APP)));
    }

    public void testWfSLASchemaNW() throws Exception {
        SchemaService wss = Services.get().get(SchemaService.class);
        Validator validator = wss.getSchema(SchemaName.WORKFLOW).newValidator();
        try {
            validator.validate(new StreamSource(new StringReader(WF_SLA_APP_NW)));
            fail("Schema service check does not work");
        }
        catch (Exception ex) {
            // Expected
        }
    }

    public void testCoordSchema() throws Exception {
        SchemaService wss = Services.get().get(SchemaService.class);
        Validator validator = wss.getSchema(SchemaName.COORDINATOR).newValidator();
        String COORD_APP1 = "<coordinator-app name='NAME' frequency='${coord:days(1)}' start='2009-02-01T01:00Z' end='2009-02-03T23:59Z' timezone='UTC' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns='uri:oozie:coordinator:0.1' xmlns:sla='uri:oozie:sla:0.1'> "
                + "<controls> <timeout>10</timeout> <concurrency>2</concurrency> <execution>LIFO</execution> </controls> <datasets> <dataset name='a' frequency='${coord:days(7)}' initial-instance='2009-02-01T01:00Z' timezone='UTC'> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> <dataset name='local_a' frequency='${coord:days(7)}' initial-instance='2009-02-01T01:00Z' timezone='UTC'> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> </datasets> <input-events> <data-in name='A' dataset='a'> <instance>${coord:latest(0)}</instance> </data-in>  </input-events> <output-events> <data-out name='LOCAL_A' dataset='local_a'> <instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> <configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> <property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> </property></configuration> </workflow>  "
                + "</action> </coordinator-app>";

        Element e = XmlUtils.parseXml(COORD_APP1);
        //System.out.println("XML :"+ XmlUtils.prettyPrint(e));
        validator.validate(new StreamSource(new StringReader(COORD_APP1)));
    }

    public void testCoordSLASchema() throws Exception {
        SchemaService wss = Services.get().get(SchemaService.class);
        Validator validator = wss.getSchema(SchemaName.COORDINATOR)
                .newValidator();
        String COORD_APP1 = "<coordinator-app name='NAME' frequency='${coord:days(1)}' start='2009-02-01T01:00Z' end='2009-02-03T23:59Z' timezone='UTC' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns='uri:oozie:coordinator:0.1' xmlns:sla='uri:oozie:sla:0.1'> "
                + "<controls> <timeout>10</timeout> <concurrency>2</concurrency> <execution>LIFO</execution> </controls> <datasets> <dataset name='a' frequency='${coord:days(7)}' initial-instance='2009-02-01T01:00Z' timezone='UTC'> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> <dataset name='local_a' frequency='${coord:days(7)}' initial-instance='2009-02-01T01:00Z' timezone='UTC'> <uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template> </dataset> </datasets> <input-events> <data-in name='A' dataset='a'> <instance>${coord:latest(0)}</instance> </data-in>  </input-events> <output-events> <data-out name='LOCAL_A' dataset='local_a'> <instance>${coord:current(-1)}</instance> </data-out> </output-events> <action> <workflow> <app-path>hdfs:///tmp/workflows/</app-path> <configuration> <property> <name>inputA</name> <value>${coord:dataIn('A')}</value> </property> <property> <name>inputB</name> <value>${coord:dataOut('LOCAL_A')}</value> </property></configuration> </workflow>  "
                + "<sla:info> <sla:app-name>5</sla:app-name> <sla:nominal-time>2009-03-06T010:00Z</sla:nominal-time> "
                + "<sla:should-start>5</sla:should-start> <sla:should-end>50</sla:should-end> "
                + "<sla:alert-contact>abc@yahoo.com</sla:alert-contact> <sla:dev-contact>abc@yahoo.com</sla:dev-contact>"
                + " <sla:qa-contact>abc@yahoo.com</sla:qa-contact> <sla:se-contact>abc@yahoo.com</sla:se-contact>"
                + "</sla:info>" + "</action> </coordinator-app>";

        Element e = XmlUtils.parseXml(COORD_APP1);
        // System.out.println("XML :"+ XmlUtils.prettyPrint(e));
        validator.validate(new StreamSource(new StringReader(COORD_APP1)));
    }

}
