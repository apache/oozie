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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.oozie.test.XTestCase;

public class TestHive2Main extends XTestCase {

    public void testJobIDPattern() {
        List<String> lines = new ArrayList<String>();
        lines.add("Ended Job = job_001");
        lines.add("Running with YARN Application = application_002");
        lines.add("Running with YARN Application = application_002_2");
        lines.add("Submitted application application_003");
        lines.add("Submitted application application_003_2");
        // Non-matching ones
        lines.add("Ended Job = ");
        lines.add("Ended Job = abc");
        lines.add("Running with YARN Application = job_004");
        lines.add("Submitted application job_006");
        Set<String> jobIds = new LinkedHashSet<String>();
        for (String line : lines) {
            LauncherMain.extractJobIDs(line, Hive2Main.HIVE2_JOB_IDS_PATTERNS,
                    jobIds);
        }
        Set<String> expected = new LinkedHashSet<String>();
        expected.add("job_001");
        expected.add("job_002");
        expected.add("job_002_2");
        expected.add("job_003");
        expected.add("job_003_2");
        assertEquals(expected, jobIds);
    }
}