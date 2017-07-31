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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestLauncherMain {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private PrintStream originalStream;

    @Before
    public void setUpStreams() {
        originalStream = System.out;
        System.setOut(new PrintStream(outContent));
    }

    @After
    public void cleanUpStreams() {
        System.setOut(originalStream);
    }

    @Test
    public void testLog4jPropertiesPresentAndReadable() {
        final LauncherMain noop = new NoopLauncherMain();
        noop.setupLog4jProperties();

        assertTrue(outContent.toString().contains("INFO: log4j config file log4j.properties loaded successfully."));
        assertEquals(noop.log4jProperties.size(), 5);
    }

    private static class NoopLauncherMain extends LauncherMain {
        @Override
        protected void run(String[] args) throws Exception {}
    }
}
