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

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URI;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestPrepareActionsHandler {
    private PrepareActionsHandler prepareActionsHandler;

    @Mock
    private LauncherURIHandlerFactory launcherURIHandlerFactory;

    @Mock
    private LauncherURIHandler launcherURIHandler;

    @Before
    public void setUp() throws LauncherException {
        prepareActionsHandler = new PrepareActionsHandler(launcherURIHandlerFactory);

        when(launcherURIHandlerFactory.getURIHandler(any(URI.class), any(Configuration.class))).thenReturn(launcherURIHandler);
    }

    @Test
    public void whenPrepareActionWithoutPrefixExecutedSuccessfully()
            throws SAXException, ParserConfigurationException, LauncherException, IOException {
        final String prepareXmlWithoutPrefix = "<prepare>\n" +
                "    <delete path=\"${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/git/oozie\"/>\n" +
                "</prepare>";

        callAndVerifyPrepareAction(prepareXmlWithoutPrefix);
    }

    private void callAndVerifyPrepareAction(final String prepareXml)
            throws IOException, SAXException, ParserConfigurationException, LauncherException {
        prepareActionsHandler.prepareAction(prepareXml, null);

        verify(launcherURIHandler).delete(any(), any());
        verifyNoMoreInteractions(launcherURIHandler);
    }

    @Test
    public void whenPrepareActionWithGitPrefixExecutedSuccessfully()
            throws SAXException, ParserConfigurationException, LauncherException, IOException {
        final String prepareXmlWithGitPrefix = "<git:prepare xmlns:git=\"uri:oozie:git-action:1.0\">\n" +
                "    <git:delete path=\"${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/git/oozie\"/>\n" +
                "</git:prepare>";

        callAndVerifyPrepareAction(prepareXmlWithGitPrefix);
    }
}