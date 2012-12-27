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

package org.apache.oozie.coord;

import java.util.Map;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.command.coord.CoordCommandUtils;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.PartitionWrapper;
import org.apache.oozie.util.PartitionsGroup;
import org.apache.oozie.util.WaitingActions;

public class TestCoordCommandUtils extends XDataTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {

        super.setUp();
        services = super.setupServicesForHCatalog();
        services.init();
        setSystemProperty(PartitionDependencyManagerService.HCAT_DEFAULT_SERVER_NAME, "myhcatserver");
        setSystemProperty(PartitionDependencyManagerService.HCAT_DEFAULT_DB_NAME, "myhcatdb");
        setSystemProperty(PartitionDependencyManagerService.MAP_MAX_WEIGHTED_CAPACITY, "100");
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testRegisterPartition() throws Exception {

        String hcatUriStr1 = "hcat://hcatserver.com:4080/mydb/mytable/datestamp=1234;region=us";
        String hcatUriStr2 = "hcat://hcatserver.com:4080/mydb/mytable/click=1234";
        CoordinatorActionBean action1 = new CoordinatorActionBean();
        action1.setId("1");
        StringBuffer st = new StringBuffer();
        st.append(hcatUriStr1);
        st.append(CoordELFunctions.INSTANCE_SEPARATOR);
        st.append(hcatUriStr2);
        st.append(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR);
        st.append("${coord:latest(0)}");
        action1.setPushMissingDependencies(st.toString());
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        String serverEndPoint = "hcat://hcatserver.com:4080";
        jmsService.getOrCreateConnection(serverEndPoint);
        CoordCommandUtils.registerPartition(action1);

        HCatURI hcatUri1 = new HCatURI(hcatUriStr1);
        HCatURI hcatUri2 = new HCatURI(hcatUriStr2);
        PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
        Map<String, Map<String, PartitionsGroup>> hcatInstanceMap = pdms.getHCatMap();
        Map<String, PartitionsGroup> tablePartitionsMap = hcatInstanceMap.get(PartitionWrapper.makePrefix(
                hcatUri1.getServer(), hcatUri1.getDb()));
        // check tablePartitionMap exist for the table
        assertTrue(tablePartitionsMap.containsKey(hcatUri1.getTable()));
        PartitionsGroup missingPartitions = tablePartitionsMap.get(hcatUri1.getTable());
        WaitingActions actions1 = missingPartitions.getPartitionsMap().get(new PartitionWrapper(hcatUri1));
        WaitingActions actions2 = missingPartitions.getPartitionsMap().get(new PartitionWrapper(hcatUri2));
        // check actionID is included in WaitingAction
        assertTrue(actions1.getActions().contains(action1.getId()));
        assertTrue(actions2.getActions().contains(action1.getId()));
    }

}
