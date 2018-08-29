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

package org.apache.oozie.util.db;

import org.junit.Assert;
import org.junit.Test;

public class TestCompositeIndex {

    @Test
    public void testisCompositIndexLowerCase() {
        Assert.assertTrue("This should be a valid composite index",
                CompositeIndex.find("i_coord_actions_job_id_status"));
    }

    @Test
    public void testisCompositIndexUpperCase() {
        Assert.assertTrue("This should be a valid composite index",
                CompositeIndex.find("I_COORD_ACTIONS_JOB_ID_STATUS"));
    }

    @Test
    public void testisCompositIndexNull() {
        Assert.assertFalse("This is not a valid composite index",
                CompositeIndex.find(null));
    }

    @Test
    public void testisCompositIndexEmptyString() {
        Assert.assertFalse("This is not a valid composite index", CompositeIndex.find(""));
    }

    @Test
    public void testIsCompositIndexNegative() {
        Assert.assertFalse("This is not a valid composite index", CompositeIndex.find("NotAnIndex"));
    }
}
