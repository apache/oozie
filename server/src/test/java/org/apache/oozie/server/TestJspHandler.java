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

package org.apache.oozie.server;

import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestJspHandler {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock File mockScratchDir;
    @Mock WebAppContext mockWebAppContext;
    @Mock WebRootResourceLocator mockWebRootResourceLocator;

    private JspHandler jspHandler;

    @Before
    public void setUp() throws Exception {
        jspHandler = new JspHandler(mockScratchDir, mockWebRootResourceLocator);
        when(mockWebRootResourceLocator.getWebRootResourceUri()).thenReturn(new URI("/webroot"));
    }

    @After
    public void tearDown() throws Exception {
        verify(mockScratchDir).exists();
    }

    @Test
    public void scratchDir_Is_Created_When_Setup_Called_And_ScratchDir_Did_Not_Exist() throws IOException, URISyntaxException {
        when(mockScratchDir.exists()).thenReturn(false);
        when(mockScratchDir.mkdirs()).thenReturn(true);

        jspHandler.setupWebAppContext(mockWebAppContext);

        verify(mockScratchDir).mkdirs();
    }

    @Test
    public void scratchDir_Cannot_Be_Created_When_Setup_Called_And_ScratchDir_Did_Not_Exist()
            throws IOException, URISyntaxException {
        when(mockScratchDir.exists()).thenReturn(false);
        when(mockScratchDir.mkdirs()).thenReturn(false);

        expectedException.expect(IOException.class);
        jspHandler.setupWebAppContext(mockWebAppContext);

        verify(mockScratchDir).mkdirs();
    }

    @Test
    public void scratchDir_Is_Reused_When_Setup_Called_And_ScratchDir_Existed() throws IOException, URISyntaxException {
        when(mockScratchDir.exists()).thenReturn(true);

        jspHandler.setupWebAppContext(mockWebAppContext);

        verify(mockScratchDir, times(0)).mkdirs();
    }
}
