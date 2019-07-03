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

package org.apache.oozie.client;



import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

public class TestWorkflowClientException {

    @Test
    public void testErrorCodeMessage() {
        OozieClientException ex = new OozieClientException("errorCode", "message");
        assertEquals("errorCode", ex.getErrorCode());
        assertEquals("message", ex.getMessage());
        assertNull(ex.getCause());
        assertTrue(ex.toString().contains("errorCode") && ex.toString().contains("message"));
    }

    @Test
    public void testErrorCodeCause() {
        OozieClientException ex = new OozieClientException("errorCode", new Exception("message"));
        assertEquals("errorCode", ex.getErrorCode());
        assertTrue(ex.getMessage().contains("message"));
        assertNotNull(ex.getCause());
        assertTrue(ex.toString().contains("errorCode") && ex.toString().contains("message"));
    }

    @Test
    public void testErrorCodeMessageCause() {
        OozieClientException ex = new OozieClientException("errorCode", "message", new Exception());
        assertEquals("errorCode", ex.getErrorCode());
        assertTrue(ex.getMessage().contains("message"));
        assertNotNull(ex.getCause());
        assertTrue(ex.toString().contains("errorCode") && ex.toString().contains("message"));
    }

}
