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

import org.apache.hadoop.io.Text;
import org.apache.oozie.test.XTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TestWritableUtils extends XTestCase {

    public void testWritableUtils() throws Exception {
        Text t = new Text();
        t.set("hello");
        byte[] array = WritableUtils.toByteArray(t);
        Text tt = WritableUtils.fromByteArray(array, Text.class);
        assertEquals("hello", tt.toString());
    }

    public void testWriteReadStr() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        WritableUtils.writeStr(dos, "a");
        WritableUtils.writeStr(dos, null);
        WritableUtils.writeStr(dos, "b");
        dos.close();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        assertEquals("a", WritableUtils.readStr(dis));
        assertNull(WritableUtils.readStr(dis));
        assertEquals("b", WritableUtils.readStr(dis));

    }
}
