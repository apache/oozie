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

package org.apache.oozie.coord.input.dependency;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XHCatTestCase;
import org.jdom.JDOMException;

import java.io.IOException;

public class TestCoordOldInputDependency extends XHCatTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = super.setupServicesForHCatalog();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testNoMissingInputDependencies() throws JDOMException, IOException, CommandException {
        final CoordinatorActionBean actionWithoutInputDependencies = createActionWithoutInputDependencies();

        assertEquals("there should be no missing dependencies",
                0,
                actionWithoutInputDependencies
                        .getPullInputDependencies()
                        .getMissingDependencies(actionWithoutInputDependencies).size());
    }

    public void testOneMissingInputDependency() throws JDOMException, IOException, CommandException {
        final CoordinatorActionBean actionWithInputDependencies = createActionWithInputDependencies();

        assertEquals("there should be one missing dependency",
                1,
                actionWithInputDependencies
                        .getPullInputDependencies()
                        .getMissingDependencies(actionWithInputDependencies).size());
    }

    private CoordinatorActionBean createActionWithoutInputDependencies() {
        final CoordinatorActionBean coordinatorAction = createAction();

        coordinatorAction.setActionXml("<coordinator-app xmlns=\"uri:oozie:coordinator:0.2\" name=\"cron-coord\"" +
                " frequency=\"0/10 * * * *\" timezone=\"UTC\" freq_timeunit=\"CRON\" end_of_duration=\"NONE\"" +
                " instance-number=\"1\" action-nominal-time=\"2010-01-01T00:00Z\" action-actual-time=\"2018-11-29T12:55Z\">\n" +
                "  <action>\n" +
                "    <workflow>\n" +
                "      <app-path>hdfs://localhost:9000/user/forsage/examples/apps/cron-schedule</app-path>\n" +
                "      <configuration>\n" +
                "        <property>\n" +
                "          <name>resourceManager</name>\n" +
                "          <value>localhost:8032</value>\n" +
                "        </property>\n" +
                "        <property>\n" +
                "          <name>nameNode</name>\n" +
                "          <value>hdfs://localhost:9000</value>\n" +
                "        </property>\n" +
                "        <property>\n" +
                "          <name>queueName</name>\n" +
                "          <value>default</value>\n" +
                "        </property>\n" +
                "      </configuration>\n" +
                "    </workflow>\n" +
                "  </action>\n" +
                "</coordinator-app>");

        return coordinatorAction;
    }

    private CoordinatorActionBean createAction() {
        final CoordinatorActionBean coordinatorAction = new CoordinatorActionBean();

        coordinatorAction.setId("0000001-181129135145907-oozie-fors-C@1");
        coordinatorAction.setJobId("0000001-181129135145907-oozie-fors-C");
        coordinatorAction.setStatus(CoordinatorAction.Status.SUCCEEDED);
        coordinatorAction.setExternalId("0000002-181129135145907-oozie-fors-W");

        return coordinatorAction;
    }

    private CoordinatorActionBean createActionWithInputDependencies() throws IOException {
        final CoordinatorActionBean action = createAction();

        action.setActionXml("<coordinator-app xmlns=\"uri:oozie:coordinator:0.2\" name=\"cron-coord\"" +
                " frequency=\"0/10 * * * *\" timezone=\"UTC\" freq_timeunit=\"CRON\" end_of_duration=\"NONE\"" +
                " instance-number=\"1\" action-nominal-time=\"2010-01-01T00:00Z\" action-actual-time=\"2018-11-29T12:55Z\">\n" +
                "    <datasets>\n" +
                "        <dataset name=\"data-1\" frequency=\"${coord:minutes(20)}\" initial-instance=\"2010-01-01T00:00Z\">\n" +
                "            <uri-template>${nameNode}/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}</uri-template>\n" +
                "        </dataset>\n" +
                "    </datasets>\n" +
                "    <input-events>\n" +
                "        <data-in name=\"input-1\" dataset=\"data-1\">\n" +
                "            <start-instance>${coord:current(-2)}</start-instance>\n" +
                "            <end-instance>${coord:current(0)}</end-instance>\n" +
                "        </data-in>\n" +
                "    </input-events>\n" +
                "  <action>\n" +
                "    <workflow>\n" +
                "      <app-path>hdfs://localhost:9000/user/forsage/examples/apps/cron-schedule</app-path>\n" +
                "      <configuration>\n" +
                "        <property>\n" +
                "          <name>resourceManager</name>\n" +
                "          <value>localhost:8032</value>\n" +
                "        </property>\n" +
                "        <property>\n" +
                "          <name>nameNode</name>\n" +
                "          <value>hdfs://localhost:9000</value>\n" +
                "        </property>\n" +
                "        <property>\n" +
                "          <name>queueName</name>\n" +
                "          <value>default</value>\n" +
                "        </property>\n" +
                "      </configuration>\n" +
                "    </workflow>\n" +
                "  </action>\n" +
                "</coordinator-app>");

        final CoordInputDependency coordPullInputDependency = CoordInputDependencyFactory
                .createPullInputDependencies(true);
        coordPullInputDependency.addUnResolvedList("data-1", "data-1");

        action.setMissingDependencies(coordPullInputDependency.serialize());
        action.setPullInputDependencies(coordPullInputDependency);

        return action;
    }
}