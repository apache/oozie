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

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestStringSerializationUtil {

    @Test
    public void testStrings() {
        for (int i = 1; i < 150000; i += 10000) {
            String value = RandomStringUtils.random(i);
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutput dataOutput = new DataOutputStream(baos);
                StringSerializationUtil.writeString(dataOutput, value);
                DataInput dataInput = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
                assertEquals("Error in serialization for size " + i,
                        value, StringSerializationUtil.readString(dataInput));
            } catch (IOException e) {
                e.printStackTrace();
                fail("Error in serialization for size " + i + "\n" + value);
            }
        }
    }
}
