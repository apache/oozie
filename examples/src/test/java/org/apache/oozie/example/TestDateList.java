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

package org.apache.oozie.example;

import org.apache.oozie.action.hadoop.security.LauncherSecurityManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDateList {
    private static final String START = "2009-02-01T01:00Z";
    private static final String END = "2009-02-01T02:00Z";
    private static final String FREQUENCY = "15";
    private static final String TIMEUNIT = "MINUTES";
    private static final String TIMEZONE = "UTC";
    private static final String EXPECTED_DATE_RANGE
            = "2009-02-01T01:00Z,2009-02-01T01:15Z,2009-02-01T01:30Z,2009-02-01T01:45Z";

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testExitStatusIs_1_IfTooFewCLIArgs() throws IOException, ParseException {
        final String[] too_few_args = {START, END, FREQUENCY, TIMEUNIT};

        LauncherSecurityManager securityManager = new LauncherSecurityManager();
        securityManager.enable();

        try {
            expectedException.expect(SecurityException.class);
            DateList.main(too_few_args);
        } finally {
            assertTrue(securityManager.getExitInvoked());
            assertEquals("Unexpected exit code.", 1, securityManager.getExitCode());
            securityManager.disable();
        }
    }

    @Test
    public void testCorrectOutput() throws IOException, ParseException {
        final String[] args = {START, END, FREQUENCY, TIMEUNIT, TIMEZONE};

        final File output_file = folder.newFile("action_output.properties");

        final String output_filename = output_file.getCanonicalPath();

        System.setProperty("oozie.action.output.properties", output_filename);

        DateList.main(args);

        Properties props = new Properties();
        try (InputStream is = new FileInputStream(output_file)) {
            props.load(is);
        }

        assertEquals("Incorrect date list.", EXPECTED_DATE_RANGE, props.getProperty("datelist"));
    }
}
