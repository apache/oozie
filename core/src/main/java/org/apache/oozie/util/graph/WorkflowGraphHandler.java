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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.Instrumentation;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class WorkflowGraphHandler extends DefaultHandler {
    private OutputStream out;
    private final OutputFormat outputFormat;
    private final WorkflowJob job;
    private boolean showKill;
    private final GraphRenderer graphRenderer;
    private final Map<String, WorkflowActionNode> tags = new LinkedHashMap<>();
    private final WorkflowParseState state = new WorkflowParseState();

    WorkflowGraphHandler(final OutputStream out,
                         final OutputFormat outputFormat,
                         final WorkflowJob job,
                         final boolean showKill,
                         final GraphRenderer graphRenderer) {
        this.out = out;
        this.job = job;
        this.showKill = showKill;
        this.graphRenderer = graphRenderer;
        this.outputFormat = outputFormat;
    }

    @Override
    public void startDocument() throws SAXException {
        // NOP
    }

    @Override
    public void endDocument() throws SAXException {
        final Instrumentation.Cron cron = new Instrumentation.Cron();
        cron.start();

        if (tags.isEmpty()) {
            // Nothing to do here!
            return;
        }

        final Map<String, WorkflowAction> workflowActions = fillWorkflowActions();
        for (final Map.Entry<String, WorkflowActionNode> entry : tags.entrySet()) {
            final String name = entry.getKey();
            final WorkflowActionNode parent = entry.getValue();
            if (workflowActions.containsKey(name)) {
                parent.setStatus(workflowActions.get(name).getStatus());
            }

            graphRenderer.addNode(parent);

            for (final Map.Entry<String, Boolean> arc : parent.getArcs().entrySet()) {
                if (!showKill && arc.getValue() && tags.get(arc.getKey()).getType().equals("kill")) {
                    // Don't show kill node (assumption: only error goes to kill node;
                    // No ok goes to kill node)
                    continue;
                }

                final WorkflowActionNode child = tags.get(arc.getKey());
                if (child != null) {
                    if (workflowActions.containsKey(arc.getKey())) {
                        child.setStatus(workflowActions.get(arc.getKey()).getStatus());
                    }

                    graphRenderer.addEdge(parent, child);
                }
            }

            graphRenderer.persist(parent);
        }

        switch (outputFormat) {
            case PNG:
                renderAndWritePng();
                break;
            case DOT:
                renderAndWriteDot();
                break;
            case SVG:
                renderAndWriteSvg();
                break;
            default:
                throw new IllegalArgumentException(String.format("Unknown outputFormat %s", outputFormat));
        }
    }

    private void renderAndWritePng() throws SAXException {
        final BufferedImage source = graphRenderer.renderPng();

        try {
            ImageIO.write(source, "png", out);
        } catch (final IOException ioe) {
            throw new SAXException(ioe);
        } finally {
            source.flush();
        }
    }

    private void renderAndWriteDot() throws SAXException {
        renderStringContent(graphRenderer.renderDot());
    }

    private void renderAndWriteSvg() throws SAXException {
        renderStringContent(graphRenderer.renderSvg());
    }

    private void renderStringContent(final String content) throws SAXException {
        Preconditions.checkState(!Strings.isNullOrEmpty(content), "No output generated from graph.");

        try {
            out.write(content.getBytes(Charsets.UTF_8));
        } catch (final IOException ioe) {
            throw new SAXException(ioe);
        }
    }

    private Map<String, WorkflowAction> fillWorkflowActions() {
        final Map<String, WorkflowAction> workflowActions = new LinkedHashMap<>();

        boolean found = false;
        for (final WorkflowAction wfAction : job.getActions()) {
            workflowActions.put(wfAction.getName(), wfAction);
            if (!found) {
                switch (wfAction.getStatus()) {
                    case KILLED:
                    case ERROR:
                    case FAILED:
                        showKill = true; // Assuming on error the workflow eventually ends with kill node
                        found = true;
                        break;
                    default:
                        // Look further
                        break;
                }
            }
        }

        return workflowActions;
    }


    @Override
    public void startElement(final String namespaceURI,
                             final String localName,
                             final String qName,
                             final Attributes atts)
            throws SAXException {
        if (localName.equalsIgnoreCase("start")) {
            final String start = localName.toLowerCase(Locale.getDefault());
            if (!tags.containsKey(start)) {
                final WorkflowActionNode v = new WorkflowActionNode(start, start);
                v.addArc(atts.getValue("to"));
                tags.put(start, v);
            }
        } else if (localName.equalsIgnoreCase("action")) {
            state.action = atts.getValue("name");
        } else if (state.action != null && state.actionType == null) {
            state.actionType = localName.toLowerCase(Locale.getDefault());
        } else if (localName.equalsIgnoreCase("ok") && state.action != null && state.actionOK == null) {
            state.actionOK = atts.getValue("to");
        } else if (localName.equalsIgnoreCase("error") && state.action != null && state.actionErr == null) {
            state.actionErr = atts.getValue("to");
        } else if (localName.equalsIgnoreCase("fork")) {
            state.fork = atts.getValue("name");
            if (!tags.containsKey(state.fork)) {
                tags.put(state.fork, new WorkflowActionNode(state.fork, localName.toLowerCase(Locale.getDefault())));
            }
        } else if (localName.equalsIgnoreCase("path")) {
            tags.get(state.fork).addArc(atts.getValue("start"));
        } else if (localName.equalsIgnoreCase("join")) {
            final String join = atts.getValue("name");
            if (!tags.containsKey(join)) {
                final WorkflowActionNode v = new WorkflowActionNode(join, localName.toLowerCase(Locale.getDefault()));
                v.addArc(atts.getValue("to"));
                tags.put(join, v);
            }
        } else if (localName.equalsIgnoreCase("decision")) {
            state.decision = atts.getValue("name");
            if (!tags.containsKey(state.decision)) {
                tags.put(state.decision, new WorkflowActionNode(state.decision, localName.toLowerCase(Locale.getDefault())));
            }
        } else if (localName.equalsIgnoreCase("case")
                || localName.equalsIgnoreCase("default")) {
            tags.get(state.decision).addArc(atts.getValue("to"));
        } else if (localName.equalsIgnoreCase("kill")
                || localName.equalsIgnoreCase("end")) {
            final String name = atts.getValue("name");
            if (!tags.containsKey(name)) {
                tags.put(name, new WorkflowActionNode(name, localName.toLowerCase(Locale.getDefault())));
            }
        }
    }


    @Override
    public void endElement(final String namespaceURI,
                           final String localName,
                           final String qName)
            throws SAXException {
        if (localName.equalsIgnoreCase("action")) {
            tags.put(state.action, new WorkflowActionNode(state.action, state.actionType));
            tags.get(state.action).addArc(state.actionOK);
            tags.get(state.action).addArc(state.actionErr, true);

            state.reset();
        }
    }

    private static class WorkflowParseState {
        private String action;
        private String actionOK;
        private String actionErr;
        private String actionType;
        private String fork;
        private String decision;

        public void reset() {
            action = null;
            actionOK = null;
            actionErr = null;
            actionType = null;
        }
    }
}