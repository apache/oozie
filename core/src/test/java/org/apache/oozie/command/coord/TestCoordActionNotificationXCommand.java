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
package org.apache.oozie.command.coord;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.wf.HangServlet;
import org.apache.oozie.command.wf.NotificationXCommand;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XConfiguration;
import org.junit.Assert;
import org.mockito.Mockito;

public class TestCoordActionNotificationXCommand extends XTestCase {
    private EmbeddedServletContainer container;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        setSystemProperty(NotificationXCommand.NOTIFICATION_URL_CONNECTION_TIMEOUT_KEY, "50");
        Services services = new Services();
        services.init();
        container = new EmbeddedServletContainer("blah");
        container.addServletEndpoint("/hang/*", HangServlet.class);
        container.start();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            container.stop();
        }
        catch (Exception ex) {
        }
        try {
            Services.get().destroy();
        }
        catch (Exception ex) {
        }
        super.tearDown();
    }

    public void testCoordNotificationTimeout() throws Exception {
        XConfiguration conf = new XConfiguration();
        conf.set(OozieClient.COORD_ACTION_NOTIFICATION_URL, container.getServletURL("/hang/*"));
        String runConf = conf.toXmlString(false);
        CoordinatorActionBean coord = Mockito.mock(CoordinatorActionBean.class);
        Mockito.when(coord.getId()).thenReturn("1");
        Mockito.when(coord.getStatus()).thenReturn(CoordinatorAction.Status.SUCCEEDED);
        Mockito.when(coord.getRunConf()).thenReturn(runConf);
        CoordActionNotificationXCommand command = new CoordActionNotificationXCommand(coord);
        command.retries = 3;
        long start = System.currentTimeMillis();
        command.call();
        long end = System.currentTimeMillis();
        Assert.assertTrue(end - start >= 50);
        Assert.assertTrue(end - start <= 100);
    }
}
