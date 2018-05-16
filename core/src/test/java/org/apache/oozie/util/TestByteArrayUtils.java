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

package org.apache.oozie.util;

import org.junit.Assert;
import org.junit.Test;

public class TestByteArrayUtils {

    @Test
    public void testByteArrayInterningGivesSameInstances() {
        final int byteArrayCount = 1000;
        final int elementCount = 100;
        final byte[][] sameContent = new byte[byteArrayCount][];

        for (int i = 0; i < byteArrayCount; i++) {
            final byte[] source = new byte[elementCount];
            sameContent[i] = source;
            for (int j = 0; j < elementCount; j++) {
                source[j] = (byte) j;
            }
        }

        for (int i = 1; i < byteArrayCount; i++) {
            Assert.assertTrue("copied byte[]s should be another instances", sameContent[i - 1] != sameContent[i]);
        }

        final byte[][] interned = new byte[byteArrayCount][];
        for (int i = 0; i < byteArrayCount; i++) {
            interned[i] = ByteArrayUtils.weakIntern(sameContent[i]);
        }

        for (int i = 1; i < byteArrayCount; i++) {
            Assert.assertTrue("weak interned byte[]s should be the same instance", interned[i - 1] == interned[i]);
        }
    }
}