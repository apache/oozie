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

package org.apache.oozie.action.hadoop;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestYarnACLHandler {
    private static final String VIEW_ACL_VALUE = "viewacl";
    private static final String MODIFY_ACL_VALUE = "modifyacl";

    private YarnACLHandler yarnACLSetter;
    private Configuration launcherConfig;

    @Mock
    private ContainerLaunchContext contextMock;

    @Captor
    private ArgumentCaptor<Map<ApplicationAccessType, String>> captor;

    @Test
    public void testViewACLisSet() {
        setupYarnACLHandler(true);
        setViewACL();

        yarnACLSetter.setACLs(contextMock);

        assertViewACLset();
    }

    @Test
    public void testModifyACLisSet() {
        setupYarnACLHandler(true);
        setModifyACL();

        yarnACLSetter.setACLs(contextMock);

        assertModifyACLset();
    }

    @Test
    public void testACLisEmptyWhenEnabledAndNoACLsDefined() {
        setupYarnACLHandler(true);

        yarnACLSetter.setACLs(contextMock);

        assertNoACLset();
    }

    @Test
    public void testACLisEmptyWhenMRACLsDisabled() {
        setupYarnACLHandler(false);
        setModifyACL();
        setViewACL();

        yarnACLSetter.setACLs(contextMock);

        assertNoACLset();
    }

    private void setupYarnACLHandler(boolean mrACLsEnabled) {
        launcherConfig = new Configuration();
        launcherConfig.setBoolean(MRConfig.MR_ACLS_ENABLED, mrACLsEnabled);

        yarnACLSetter = new YarnACLHandler(launcherConfig);
    }

    private void setViewACL() {
        launcherConfig.set(JavaActionExecutor.LAUNCER_VIEW_ACL, VIEW_ACL_VALUE);
    }

    private void setModifyACL() {
        launcherConfig.set(JavaActionExecutor.LAUNCER_MODIFY_ACL, MODIFY_ACL_VALUE);
    }

    private void assertModifyACLset() {
        verifyACLset("Modify ACL", MODIFY_ACL_VALUE, ApplicationAccessType.MODIFY_APP);
    }

    private void assertViewACLset() {
        verifyACLset("View ACL", VIEW_ACL_VALUE, ApplicationAccessType.VIEW_APP);
    }

    @SuppressWarnings("unchecked")
    private void assertNoACLset() {
        verify(contextMock, never()).setApplicationACLs(any(Map.class));
    }

    private void verifyACLset(String aclMessage, String aclValue, ApplicationAccessType aclType) {
        verify(contextMock).setApplicationACLs(captor.capture());
        Map<ApplicationAccessType, String> aclDefinition = captor.getValue();
        assertEquals("ACL size", 1, aclDefinition.size());
        assertEquals(aclMessage, aclValue, aclDefinition.get(aclType));
    }
}