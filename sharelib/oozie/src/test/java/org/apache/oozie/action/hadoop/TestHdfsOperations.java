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
import static org.mockito.Matchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class TestHdfsOperations {
    @Mock
    private SequenceFileWriterFactory seqFileWriterFactoryMock;

    @Mock
    private SequenceFile.Writer writerMock;

    @Mock
    private Configuration configurationMock;

    private Path path = new Path(".");

    private Map<String, String> actionData = new HashMap<>();

    @InjectMocks
    private HdfsOperations hdfsOperations;

    @Before
    public void setup() throws Exception {
        configureMocksForHappyPath();
        actionData.put("testKey", "testValue");
    }

    @Test
    public void testActionDataUploadToHdfsSucceeds() throws Exception {
        hdfsOperations.uploadActionDataToHDFS(configurationMock, path, actionData);

        verify(seqFileWriterFactoryMock).createSequenceFileWriter(eq(configurationMock),
                any(Path.class), eq(Text.class), eq(Text.class));
        ArgumentCaptor<Text> keyCaptor = ArgumentCaptor.forClass(Text.class);
        ArgumentCaptor<Text> valueCaptor = ArgumentCaptor.forClass(Text.class);
        verify(writerMock).append(keyCaptor.capture(), valueCaptor.capture());
        assertEquals("testKey", keyCaptor.getValue().toString());
        assertEquals("testValue", valueCaptor.getValue().toString());
    }

    @Test(expected = IOException.class)
    public void testActionDataUploadToHdfsFailsWhenAppendingToWriter() throws Exception {
        willThrow(new IOException()).given(writerMock).append(any(Text.class), any(Text.class));

        hdfsOperations.uploadActionDataToHDFS(configurationMock, path, actionData);
    }

    @Test(expected = IOException.class)
    public void testActionDataUploadToHdfsFailsWhenWriterIsNull() throws Exception {
        given(seqFileWriterFactoryMock.createSequenceFileWriter(eq(configurationMock),
                any(Path.class), eq(Text.class), eq(Text.class))).willReturn(null);

        hdfsOperations.uploadActionDataToHDFS(configurationMock, path, actionData);
    }

    @SuppressWarnings("unchecked")
    private void configureMocksForHappyPath() throws Exception {
        given(seqFileWriterFactoryMock.createSequenceFileWriter(eq(configurationMock),
                any(Path.class), eq(Text.class), eq(Text.class))).willReturn(writerMock);
    }
}
