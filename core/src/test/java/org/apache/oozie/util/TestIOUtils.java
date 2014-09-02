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

import org.apache.oozie.test.XTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;

public class TestIOUtils extends XTestCase {

    public void testGetReaderAsString() throws Exception {
        try {
            IOUtils.getReaderAsString(new StringReader("1234"), 2);
            fail();
        }
        catch (IllegalArgumentException ex) {
            //nop
        }
        assertEquals("1234", IOUtils.getReaderAsString(new StringReader("1234"), 4));
    }

    public void testGetResourceAsString() throws Exception {
        try {
            IOUtils.getResourceAsString("invalid-resource.txt", 2);
            fail();
        }
        catch (IllegalArgumentException ex) {
            //nop
        }
        String s = IOUtils.getResourceAsString("test-ioutils.txt", 10);
        assertEquals("abcde", s);
        try {
            IOUtils.getResourceAsString("test-ioutils.txt", 2);
            fail();
        }
        catch (IllegalArgumentException ex) {
            //nop
        }
    }

    public void testGetResourceAsReader() throws Exception {
        IOUtils.getResourceAsReader("test-ioutils.txt", 10);
    }

    public void testCopyStream() throws IOException {
        byte[] original = new byte[]{0, 1, 2};
        ByteArrayInputStream is = new ByteArrayInputStream(original);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        IOUtils.copyStream(is, os);
        byte[] copy = os.toByteArray();
        assertEquals(3, copy.length);
        assertEquals(original.length, copy.length);
        for (int i = 0; i < original.length; i++) {
            assertEquals(original[i], copy[i]);
        }
    }

}
