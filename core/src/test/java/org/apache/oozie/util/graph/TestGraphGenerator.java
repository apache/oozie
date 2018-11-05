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

package org.apache.oozie.util.graph;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.internal.AssumptionViolatedException;

import javax.imageio.ImageIO;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;

public class TestGraphGenerator extends XTestCase {
    private static final XLog LOG = XLog.getLog(TestGraphGenerator.class);

    private static final String GRAPH_WORKFLOW_DECISION_FORK_JOIN_XML = "graph-workflow-decision-fork-join.xml";
    private static final String GRAPH_WORKFLOW_MANY_ACTIONS_XML = "graph-workflow-many-actions.xml";
    private static final String GRAPH_WORKFLOW_SIMPLE_XML = "graph-workflow-simple.xml";
    private static final String GRAPH_WORKFLOW_INVALID_XML = "graph-workflow-invalid.xml";

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();

        super.tearDown();
    }

    public void testSimpleGraphPng() {
        try {
            assumeJDKVersion();
        }
        catch (final AssumptionViolatedException ave) {
            // Due to JUnit38ClassRunner we have to check that explicitly instead of relying on junit.framework
            LOG.warn(ave.getMessage());
            return;
        }

        final WorkflowJobBean jsonWFJob = createSimpleWorkflow();

        generateAndAssertPng(jsonWFJob, GRAPH_WORKFLOW_SIMPLE_XML, false);

        generateAndAssertPng(jsonWFJob, GRAPH_WORKFLOW_SIMPLE_XML, true);

        try {
            final String content = IOUtils.getResourceAsString(GRAPH_WORKFLOW_INVALID_XML, -1);
            final GraphGenerator g = new GraphGenerator(content, jsonWFJob, true, new GraphvizRenderer());
            g.write(new NullOutputStream(), OutputFormat.PNG);
        }
        catch (final Exception e) {
            Assert.fail("Write PNG shouldn't have failed for graph-workflow-invalid.xml call: " + e.getMessage());
        }
    }

    private WorkflowJobBean createSimpleWorkflow() {
        final WorkflowJobBean jsonWFJob = new WorkflowJobBean();
        jsonWFJob.setAppName("My Test App");
        jsonWFJob.setId("My Test ID");
        return jsonWFJob;
    }

    private void generateAndAssertPng(final WorkflowJobBean workflowJob, final String path, final boolean showKill) {
        File outputPng = null;
        try {
            outputPng = File.createTempFile("graph-output", path + ".png");
            final String content = IOUtils.getResourceAsString(path, -1);
            final GraphGenerator g = new GraphGenerator(content, workflowJob, showKill, new GraphvizRenderer());
            g.write(new FileOutputStream(outputPng), OutputFormat.PNG);

            Assert.assertNotNull("PNG read error", ImageIO.read(new FileInputStream(outputPng)));
        }
        catch (final Exception e) {
            Assert.fail(String.format("Render and write PNG failed for %s: %s", path, e.getMessage()));
        }
        finally {
            if (outputPng != null) {
                outputPng.delete();
            }
        }
    }

    public void testSimpleGraphDot() {
        try {
            assumeJDKVersion();
        }
        catch (final AssumptionViolatedException ave) {
            // Due to JUnit38ClassRunner we have to check that explicitly instead of relying on junit.framework
            LOG.warn(ave.getMessage());
            return;
        }

        final WorkflowJobBean jsonWFJob = createSimpleWorkflow();

        File outputDot = null;
        try {
            outputDot = File.createTempFile("graph-output", "graph-workflow-simple.dot");
            final String content = IOUtils.getResourceAsString("graph-workflow-simple.xml", -1);
            final GraphGenerator g = new GraphGenerator(content, jsonWFJob, true, new GraphvizRenderer());
            g.write(new FileOutputStream(outputDot), OutputFormat.DOT);

            final BufferedReader dotReader = new BufferedReader(new FileReader(outputDot));
            Assert.assertTrue("Rendered and written graph output file is not a DOT file",
                    dotReader.readLine().equals("digraph {"));
        }
        catch (final Exception e) {
            Assert.fail("Render and write DOT failed: " + e.getMessage());
        }
        finally {
            if (outputDot != null) {
                outputDot.delete();
            }
        }
    }

    public void testSimpleGraphSvg() {
        try {
            assumeJDKVersion();
        }
        catch (final AssumptionViolatedException ave) {
            // Due to JUnit38ClassRunner we have to check that explicitly instead of relying on junit.framework
            LOG.warn(ave.getMessage());
            return;
        }

        final WorkflowJobBean jsonWFJob = createSimpleWorkflow();

        File outputDot = null;
        try {
            outputDot = File.createTempFile("graph-output", "graph-workflow-simple.svg");
            final String content = IOUtils.getResourceAsString("graph-workflow-simple.xml", -1);
            final GraphGenerator g = new GraphGenerator(content, jsonWFJob, true, new GraphvizRenderer());
            g.write(new FileOutputStream(outputDot), OutputFormat.SVG);

            final BufferedReader svgReader = new BufferedReader(new FileReader(outputDot));
            Assert.assertTrue("Rendered and written graph output file is not an SVG file",
                    svgReader.readLine().startsWith("<svg "));
        }
        catch (final Exception e) {
            Assert.fail("Render and write SVG failed: " + e.getMessage());
        }
        finally {
            if (outputDot != null) {
                outputDot.delete();
            }
        }
    }

    public void testGraphWithManyNodes() throws Exception {
        try {
            assumeJDKVersion();
        }

        catch (final AssumptionViolatedException ave) {
            // Due to JUnit38ClassRunner we have to check that explicitly instead of relying on junit.framework
            LOG.warn(ave.getMessage());
            return;
        }
        new GraphGenerator(readXmlFromClasspath(GRAPH_WORKFLOW_MANY_ACTIONS_XML),
                createWorkflowInProgress(25),
                true,
                new GraphvizRenderer())
                .write(new NullOutputStream(), OutputFormat.PNG);
    }

    private String readXmlFromClasspath(final String classpathLocation) throws IOException {
        return IOUtils.getResourceAsString(classpathLocation, -1);
    }

    public void testGraphWithDecisionForkJoin() throws Exception {
        try {
            assumeJDKVersion();
        }
        catch (final AssumptionViolatedException ave) {
            // Due to JUnit38ClassRunner we have to check that explicitly instead of relying on junit.framework
            LOG.warn(ave.getMessage());
            return;
        }

        new GraphGenerator(readXmlFromClasspath(GRAPH_WORKFLOW_DECISION_FORK_JOIN_XML),
                createWorkflowWithDecisionForkJoin(),
                true,
                new GraphvizRenderer())
                .write(new NullOutputStream(), OutputFormat.PNG);
    }

    private WorkflowJobBean createWorkflowInProgress(final int actionNodeCount) {
        final WorkflowJobBean workflowJobBean = createSimpleWorkflow();

        workflowJobBean.getActions().add(createAction("start", "start", WorkflowAction.Status.DONE));
        final int ixRunning = (int) Math.floor(Math.random() * actionNodeCount);
        for (int ixNode = 0; ixNode < actionNodeCount; ixNode++) {
            final WorkflowAction.Status status;
            if (ixNode < ixRunning) {
                status = WorkflowAction.Status.DONE;
            }
            else if (ixNode == ixRunning) {
                status = WorkflowAction.Status.RUNNING;
            }
            else {
                status = WorkflowAction.Status.PREP;
            }
            workflowJobBean.getActions().add(createAction(String.format("pig-%d", ixNode), "pig", status));
        }
        workflowJobBean.getActions().add(createAction("kill", "kill", WorkflowAction.Status.PREP));
        workflowJobBean.getActions().add(createAction("end", "end", WorkflowAction.Status.PREP));

        return workflowJobBean;
    }

    private WorkflowJobBean createWorkflowWithDecisionForkJoin() {
        final WorkflowJobBean workflowJobBean = createSimpleWorkflow();

        workflowJobBean.getActions().add(createAction("start", "start", WorkflowAction.Status.DONE));
        workflowJobBean.getActions().add(createAction("decision", "decision", WorkflowAction.Status.DONE));
        workflowJobBean.getActions().add(createAction("pig-0", "pig", WorkflowAction.Status.PREP));
        workflowJobBean.getActions().add(createAction("fork", "fork", WorkflowAction.Status.DONE));
        workflowJobBean.getActions().add(createAction("pig-1", "pig", WorkflowAction.Status.DONE));
        workflowJobBean.getActions().add(createAction("pig-2", "pig", WorkflowAction.Status.RUNNING));
        workflowJobBean.getActions().add(createAction("join", "join", WorkflowAction.Status.PREP));
        workflowJobBean.getActions().add(createAction("kill", "kill", WorkflowAction.Status.PREP));
        workflowJobBean.getActions().add(createAction("end", "end", WorkflowAction.Status.PREP));

        return workflowJobBean;
    }

    private WorkflowAction createAction(final String name, final String type, final WorkflowAction.Status status) {
        final WorkflowActionBean workflowActionBean = new WorkflowActionBean();
        workflowActionBean.setName(name);
        workflowActionBean.setType(type);
        workflowActionBean.setStatus(status);

        return workflowActionBean;
    }

    /**
     * Due to {@code guru.nidi:graphviz-java} >= 0.5.1 we need to check whether we have the proper minor version when running on
     * JDK8.
     * @see <a href="https://github.com/nidi3/graphviz-java/commit/b7cf5761f97f1491d3bdc65367ec00e38d66291d">this commit</a>
     */
    private void assumeJDKVersion() {
        final String version = System.getProperty("java.version");
        if (version.startsWith("1.8.0_")) {
            try {
                Assume.assumeTrue("An old version of Java 1.8 is used, skipping.", Integer.parseInt(version.substring(6)) >= 40);
            }
            catch (final NumberFormatException ignored) {}
        }
    }

    private static class NullOutputStream extends OutputStream {
        @Override
        public void write(final int b) throws IOException {
        }
    }
}
