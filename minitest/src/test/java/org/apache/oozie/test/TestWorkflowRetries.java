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

package org.apache.oozie.test;

import org.apache.oozie.util.db.FailingHSQLDBDriverWrapper;

/**
 * {@code MiniOozie} integration test for different workflow kinds, using a HSQLDB driver that fails sometimes.
 */
public class TestWorkflowRetries extends TestWorkflow {

    @Override
    protected void setUp() throws Exception {
        System.setProperty(FailingHSQLDBDriverWrapper.USE_FAILING_DRIVER, Boolean.TRUE.toString());

        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        System.setProperty(FailingHSQLDBDriverWrapper.USE_FAILING_DRIVER, Boolean.FALSE.toString());
    }
}