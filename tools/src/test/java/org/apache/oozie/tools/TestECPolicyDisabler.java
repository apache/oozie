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

package org.apache.oozie.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies.ReplicationPolicy;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.oozie.tools.ECPolicyDisabler.Result;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Test for the Erasure Coding disabler code.
 *
 * Noted that getErasureCodingPolicy was introduced to hadoop 3 and the returned class (ErasureCodingPolicy) does
 * NOT exist in hadoop 2. That is to say, the implementation of getErasureCodingPolicy can't work with both hadoop 2
 * and hadoop 3. Hence, this test makes ECPolicyDisabler#check to test another method `getFakeErasureCodingPolicy`
 */
public class TestECPolicyDisabler  {

    private static final String FAKE_METHOD_NAME = "getFakeErasureCodingPolicy";

    static abstract class MockDistributedFileSystem extends DistributedFileSystem {
        public abstract SystemErasureCodingPolicies.ReplicationPolicy getFakeErasureCodingPolicy(Path path);
        public abstract void setErasureCodingPolicy(Path path, String policy);
    }

    @Before
    public void setup() {
        SystemErasureCodingPolicies.setSystemPolicy(ReplicationPolicy.DEFAULT);
    }

    @Test
    public void testNotSupported() {
        FileSystem fs = mock(FileSystem.class);
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, null, FAKE_METHOD_NAME);
        Assert.assertEquals("result is expected", Result.NOT_SUPPORTED, result);
    }

    @Test
    public void testOkNotChanged() {
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getFakeErasureCodingPolicy(any())).thenReturn(ReplicationPolicy.DEFAULT);
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, null, FAKE_METHOD_NAME);
        assertEquals("result is expected", Result.ALREADY_SET, result);
        verify(fs).getFakeErasureCodingPolicy(any());
        verifyNoMoreInteractions(fs);
    }

    @Test
    public void testOkChanged() {
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getFakeErasureCodingPolicy(any())).thenReturn(ReplicationPolicy.OTHER);
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, null, FAKE_METHOD_NAME);
        assertEquals("result is expected", Result.DONE, result);
        verify(fs).getFakeErasureCodingPolicy(any());
        verify(fs).setErasureCodingPolicy(any(), eq("DEFAULT"));
        verifyNoMoreInteractions(fs);
    }

    @Test
    public void testServerNotSupports() {
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getFakeErasureCodingPolicy(any())).thenReturn(ReplicationPolicy.OTHER);
        Mockito.doThrow(createNoSuchMethodException()).when(fs).setErasureCodingPolicy(any(), any());
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, null, FAKE_METHOD_NAME);
        assertEquals("result is expected", Result.NO_SUCH_METHOD, result);
        verify(fs).getFakeErasureCodingPolicy(any());
        verify(fs).setErasureCodingPolicy(any(), eq("DEFAULT"));
        verifyNoMoreInteractions(fs);
    }

    @Test
    public void testServerNotSupportsGetErasureCodingPolicyMethod() {
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getFakeErasureCodingPolicy(any(Path.class))).thenThrow(createNoSuchMethodException());
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, mock(Path.class), FAKE_METHOD_NAME);
        assertEquals("result is expected", Result.NO_SUCH_METHOD, result);
        verify(fs).getFakeErasureCodingPolicy(any(Path.class));
        verifyNoMoreInteractions(fs);
    }

    @Test
    public void testServerNotSupportsGetName() {
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getFakeErasureCodingPolicy(any())).thenReturn(ReplicationPolicy.OTHER);

        ReplicationPolicy mockPolicy = mock(ReplicationPolicy.class);
        SystemErasureCodingPolicies.setSystemPolicy(mockPolicy);
        when(mockPolicy.getName()).thenThrow(createNoSuchMethodException());
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, null, FAKE_METHOD_NAME);
        assertEquals("result is expected", Result.NO_SUCH_METHOD, result);
        verify(fs).getFakeErasureCodingPolicy(any());
        verifyNoMoreInteractions(fs);
    }

    @Test
    public void testServerNotSupportsSetErasureCodingPolicyMethod() {
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getFakeErasureCodingPolicy(any())).thenReturn(ReplicationPolicy.OTHER);
        Mockito.doThrow(createNoSuchMethodException()).when(fs).setErasureCodingPolicy(any(), any());
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, null, FAKE_METHOD_NAME);
        assertEquals("result is expected", Result.NO_SUCH_METHOD, result);
        verify(fs).getFakeErasureCodingPolicy(any());
        verify(fs).setErasureCodingPolicy(any(), eq("DEFAULT"));
        verifyNoMoreInteractions(fs);
    }

    @Test
    public void testOtherRuntimeExceptionThrown() {
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getFakeErasureCodingPolicy(any())).thenReturn(ReplicationPolicy.OTHER);
        Mockito.doThrow(new RuntimeException("mock io exception")).when(fs).setErasureCodingPolicy(any(), any());
        try {
            ECPolicyDisabler.check(fs, null, FAKE_METHOD_NAME);
            Assert.fail("exception expected");
        } catch (RuntimeException e) {
            assertNotNull("runtime exception got", e);
        }
        verify(fs).getFakeErasureCodingPolicy(any());
        verify(fs).setErasureCodingPolicy(any(), eq("DEFAULT"));
        verifyNoMoreInteractions(fs);
    }

    private RuntimeException createNoSuchMethodException() {
        return new RuntimeException(new RemoteException("test", "error", RpcErrorCodeProto.ERROR_NO_SUCH_METHOD));
    }
}
