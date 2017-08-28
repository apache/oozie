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

import junit.framework.Assert;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.test.XTestCase;

import javax.imageio.ImageIO;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestGraphGenerator extends XTestCase {

    public void testConstructor() {
        try {
            new GraphGenerator(null, null);
        }
        catch (final IllegalArgumentException iae) {
            Assert.assertTrue("Construction with illegal args failed as expected: " + iae.getMessage(), true);
        }
        try {
            new GraphGenerator("<workflow></workflow>", null);
        }
        catch (final IllegalArgumentException iae) {
            Assert.assertTrue("Construction with illegal args failed as expected: " + iae.getMessage(), true);
        }
        Assert.assertNotNull(new GraphGenerator("<workflow></workflow>", new WorkflowJobBean()));
        Assert.assertNotNull(new GraphGenerator(null, new WorkflowJobBean()));
        final WorkflowJobBean jsonWFJob = new WorkflowJobBean();
        jsonWFJob.setAppName("My Test App");
        jsonWFJob.setId("My Test ID");
        Assert.assertNotNull(new GraphGenerator("<workflow></workflow>", jsonWFJob));
        Assert.assertNotNull(new GraphGenerator("<workflow></workflow>", jsonWFJob, false));
        Assert.assertNotNull(new GraphGenerator("<workflow></workflow>", jsonWFJob, true));
    }

    public void testWrite() {
        final WorkflowJobBean jsonWFJob = new WorkflowJobBean();
        jsonWFJob.setAppName("My Test App");
        jsonWFJob.setId("My Test ID");

        generateAndAssertPng(jsonWFJob, "graphWF.xml", false);

        generateAndAssertPng(jsonWFJob, "graphWF.xml", true);

        try {
            final String content = IOUtils.getResourceAsString("invalidGraphWF.xml", -1);
            final GraphGenerator g = new GraphGenerator(content, jsonWFJob, true);
            g.write(new org.apache.hadoop.io.IOUtils.NullOutputStream());
        }
        catch (final Exception e) {
            Assert.fail("Write PNG failed for invalidGraphWF.xml: " + e.getMessage());
        }
    }

    private void generateAndAssertPng(final WorkflowJobBean workflowJob, final String path, final boolean showKill) {
        try {
            final File outputPng = File.createTempFile("graph-output", path);
            final String content = IOUtils.getResourceAsString(path, -1);
            final GraphGenerator g = new GraphGenerator(content, workflowJob);
            g.write(new FileOutputStream(outputPng));
            Assert.assertNotNull("PNG read error", ImageIO.read(new FileInputStream(outputPng)));
        }
        catch (final Exception e) {
            Assert.fail(String.format("Read or write PNG without kill failed for %s: %s", path, e.getMessage()));
        }
    }

    public void testJobDAGLimit_more() throws IOException {
        final WorkflowJobBean jsonWFJob = new WorkflowJobBean();
        jsonWFJob.setAppName("My Test App");
        jsonWFJob.setId("My Test ID");

        try {
            final String content = IOUtils.getResourceAsString("graphWF_26_actions.xml", -1);
            final GraphGenerator g = new GraphGenerator(content, jsonWFJob);
            g.write(new FileOutputStream(File.createTempFile("graph-output", "over-limit")));
            Assert.fail("This should not get executed");

        }
        catch (final Exception e) {
            Assert.assertTrue(e.getMessage().startsWith(
                    "Can't display the graph. Number of actions are more than display limit"));
        }
    }
}
