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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.jdom.Element;
import org.jdom.Namespace;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.charset.StandardCharsets;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Services.class)
public class TestScriptLanguageActionExecutor {

    @Mock private ActionExecutor.Context mockContext;
    @Mock private Element mockScript;
    @Mock private Element mockElement;
    @Mock private Path mockPath;
    @Mock private Configuration mockConfiguration;
    @Mock private FSDataOutputStream fsDataOutputStream;
    @Mock private FileSystem mockFs;
    @Mock private Configuration mockActionConfig;
    @Mock private Services mockServices;

    @Before
    public void setup() throws ServiceException {
        PowerMockito.mockStatic(Services.class);

        when(Services.get()).thenReturn(mockServices);
        doReturn(mockConfiguration).when(mockServices).getConf();
    }

    @Test
    public void multibyteInputsAreAcceptedInScripts() throws Exception {
        final String testInput = "林檎";
        doReturn(mockScript).when(mockElement).getChild(anyString(), any(Namespace.class));
        doReturn("script").when(mockScript).getTextTrim();
        doReturn(mockActionConfig).when(mockContext).getProtoActionConf();
        doReturn(testInput).when(mockActionConfig).get(anyString());
        doReturn(new Path(".")).when(mockContext).getActionDir();
        doReturn(mockFs).when(mockContext).getAppFileSystem();
        doReturn(fsDataOutputStream).when(mockFs).create(any(Path.class));

        ScriptLanguageActionExecutor scriptLanguageActionExecutor = spy(new ScriptLanguageActionExecutor("pig") {
            @Override
            protected String getScriptName() {
                return null;
            }
        });
        scriptLanguageActionExecutor.addScriptToCache(mockConfiguration, mockElement, mockPath, mockContext);
        byte[] expectedInput = testInput.getBytes(StandardCharsets.UTF_8);
        verify(fsDataOutputStream).write(expectedInput);
    }

    @After
    public void cleanUp() {
        Services.get().destroy();
    }
}
