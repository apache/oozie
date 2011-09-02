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
package org.apache.oozie.command.coord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XTestCase;

public class TestCoordActionStartCommand extends XTestCase {
    private Services services;

    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }


    public void testActionStartCommand() throws StoreException,
            CommandException, IOException {
        CoordinatorStore store = Services.get().get(StoreService.class)
                .getStore(CoordinatorStore.class);
        String actionId = new Date().getTime() + "-COORD-ActionStartCommand-C@1";
        try {
            addRecordToActionTable(actionId, 1, store);
        }
        finally {
            store.closeTrx();
        }
        new CoordActionStartCommand(actionId, "me", "mytoken").call();
        checkCoordAction(actionId);
    }

    private void addRecordToActionTable(String actionId, int actionNum,
                                        CoordinatorStore store) throws StoreException, IOException {
        // CoordinatorStore store = new CoordinatorStore(false);
        CoordinatorActionBean action = new CoordinatorActionBean();
        action.setJobId(actionId);
        action.setId(actionId);
        action.setActionNumber(actionNum);
        action.setNominalTime(new Date());
        action.setStatus(Status.SUBMITTED);
        String appPath = "/tmp/coord/no-op/";
        String actionXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' xmlns:sla='uri:oozie:sla:0.1' name='NAME' frequency=\"1\" start='2009-02-01T01:00Z' end='2009-02-03T23:59Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'  instance-number=\"1\" action-nominal-time=\"2009-02-01T01:00Z\">";
        actionXml += "<controls>";
        actionXml += "<timeout>10</timeout>";
        actionXml += "<concurrency>2</concurrency>";
        actionXml += "<execution>LIFO</execution>";
        actionXml += "</controls>";
        actionXml += "<input-events>";
        actionXml += "<data-in name='A' dataset='a'>";
        actionXml += "<dataset name='a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        actionXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        actionXml += "</dataset>";
        actionXml += "<instance>${coord:latest(0)}</instance>";
        actionXml += "</data-in>";
        actionXml += "</input-events>";
        actionXml += "<output-events>";
        actionXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        actionXml += "<dataset name='local_a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        actionXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        actionXml += "</dataset>";
        actionXml += "<instance>${coord:current(-1)}</instance>";
        actionXml += "</data-out>";
        actionXml += "</output-events>";
        actionXml += "<action>";
        actionXml += "<workflow>";
        actionXml += "<app-path>file://" + appPath + "</app-path>";
        actionXml += "<configuration>";
        actionXml += "<property>";
        actionXml += "<name>inputA</name>";
        actionXml += "<value>file:///tmp/coord//US/2009/02/01</value>";
        actionXml += "</property>";
        actionXml += "<property>";
        actionXml += "<name>inputB</name>";
        actionXml += "<value>file:///tmp/coord//US/2009/02/01</value>";
        actionXml += "</property>";
        actionXml += "</configuration>";
        actionXml += "</workflow>";
        String slaXml = " <sla:info xmlns:sla='uri:oozie:sla:0.1'>"
                + " <sla:app-name>test-app</sla:app-name>"
                + " <sla:nominal-time>2009-03-06T10:00Z</sla:nominal-time>"
                + " <sla:should-start>5</sla:should-start>"
                + " <sla:should-end>120</sla:should-end>"
                + " <sla:notification-msg>Notifying User for nominal time : 2009-03-06T10:00Z </sla:notification-msg>"
                + " <sla:alert-contact>abc@yahoo.com</sla:alert-contact>"
                + " <sla:dev-contact>abc@yahoo.com</sla:dev-contact>"
                + " <sla:qa-contact>abc@yahoo.com</sla:qa-contact>"
                + " <sla:se-contact>abc@yahoo.com</sla:se-contact>"
                + "</sla:info>";
        actionXml += slaXml;
        actionXml += "</action>";
        actionXml += "</coordinator-app>";
        action.setActionXml(actionXml);
        action.setSlaXml(slaXml);

        String createdConf = "<configuration> ";
        createdConf += "<property> <name>execution_order</name> <value>LIFO</value> </property>";
        createdConf += "<property> <name>user.name</name> <value>" + getTestUser() + "</value> </property>";
        createdConf += "<property> <name>group.name</name> <value>other</value> </property>";
        createdConf += "<property> <name>app-path</name> " + "<value>file://"
                + appPath + "/</value> </property>";
        createdConf += "<property> <name>jobTracker</name> ";
        createdConf += "<value>localhost:9001</value></property>";
        createdConf += "<property> <name>nameNode</name> <value>hdfs://localhost:9000</value></property>";
        createdConf += "<property> <name>queueName</name> <value>default</value></property>";

        createdConf += "</configuration> ";

        action.setCreatedConf(createdConf);
        store.beginTrx();
        store.insertCoordinatorAction(action);
        store.commitTrx();
        String content = "<workflow-app xmlns='uri:oozie:workflow:0.1'  xmlns:sla='uri:oozie:sla:0.1' name='no-op-wf'>";
        content += "<start to='end' />";
        String slaXml2 = " <sla:info>"
                //+ " <sla:client-id>axonite-blue</sla:client-id>"
                + " <sla:app-name>test-app</sla:app-name>"
                + " <sla:nominal-time>2009-03-06T10:00Z</sla:nominal-time>"
                + " <sla:should-start>5</sla:should-start>"
                + " <sla:should-end>${2 * HOURS}</sla:should-end>"
                + " <sla:notification-msg>Notifying User for nominal time : 2009-03-06T10:00Z </sla:notification-msg>"
                + " <sla:alert-contact>abc@yahoo.com</sla:alert-contact>"
                + " <sla:dev-contact>abc@yahoo.com</sla:dev-contact>"
                + " <sla:qa-contact>abc@yahoo.com</sla:qa-contact>"
                + " <sla:se-contact>abc@yahoo.com</sla:se-contact>"
                + "</sla:info>";
        content += "<end name='end' />" + slaXml2 + "</workflow-app>";
        writeToFile(content, appPath);
        //System.out.println("COMMITED TRX");
    }

    private void checkCoordAction(String actionId) throws StoreException {
        CoordinatorStore store = Services.get().get(StoreService.class)
                .getStore(CoordinatorStore.class);
        try {
            store.beginTrx();
            CoordinatorActionBean action = store.getCoordinatorAction(actionId,
                                                                      true);
            if (action.getStatus() == CoordinatorAction.Status.SUBMITTED) {
                fail("CoordActionStartCommand didn't work because the status for action id"
                        + actionId
                        + " is :"
                        + action.getStatus()
                        + " expected to be NOT SUBMITTED (i.e. RUNNING)");
            }

        }
        catch (StoreException se) {
            fail("Action ID " + actionId + " was not stored properly in db");
        }
        finally {
            store.commitTrx();
            store.closeTrx();
        }
    }

    private void writeToFile(String content, String appPath) throws IOException {
        createDir(appPath);
        File wf = new File(appPath + "/workflow.xml");
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(wf));
            out.println(content);
        }
        catch (IOException iex) {
            iex.printStackTrace();
            throw iex;
        }
        finally {
            if (out != null) {
                out.close();
            }
        }

    }

    private void createDir(String dir) {
        Process pr;
        try {
            pr = Runtime.getRuntime().exec("mkdir -p " + dir + "/_SUCCESS");
            pr.waitFor();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

