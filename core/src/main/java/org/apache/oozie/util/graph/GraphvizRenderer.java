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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.attribute.RankDir;
import guru.nidi.graphviz.attribute.Shape;
import guru.nidi.graphviz.engine.Engine;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.engine.Rasterizer;
import guru.nidi.graphviz.model.Factory;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.Node;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;

import java.awt.image.BufferedImage;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class GraphvizRenderer implements GraphRenderer {

    /**
     * We need this single-thread executor because we have to make sure:
     * <ul>
     * <li>all GraphViz rendering operations happen on the same thread.
     * This is because of {@link com.eclipsesource.v8.V8} thread handling</li>
     * <li>GraphViz rendering operations don't timeout</li>
     * <li>GraphViz rendering operations don't overlap</li>
     * </ul>
     */
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
    private static final long GRAPHVIZ_TIMEOUT_SECONDS = ConfigurationService.getLong("oozie.graphviz.timeout.seconds");

    private Graph graphvizGraph = Factory.graph().graphAttr().with(RankDir.TOP_TO_BOTTOM).directed();
    private final Map<String, Node> graphvizNodes = new LinkedHashMap<>();
    private final Multimap<String, String> edges = ArrayListMultimap.create();
    private int arcCount = 0;

    @Override
    public void addNode(final WorkflowActionNode node) {
        final Shape shape = getShape(node.getType());
        final Color color = getColor(node.getStatus());

        final Node graphvizNode = Factory.node(node.getName()).with(shape).with(color);

        graphvizNodes.put(node.getName(), graphvizNode);
    }

    private Shape getShape(final String type) {
        final Shape shape;

        switch (type) {
            case "start":
                shape = Shape.CIRCLE;
                break;
            case "end":
                shape = Shape.DOUBLE_CIRCLE;
                break;
            case "kill":
                shape = Shape.OCTAGON;
                break;
            case "decision":
                shape = Shape.DIAMOND;
                break;
            case "fork":
                shape = Shape.TRIANGLE;
                break;
            case "join":
                shape = Shape.INV_TRIANGLE;
                break;
            default:
                shape = Shape.RECTANGLE;
                break;
        }

        return shape;
    }

    private Color getColor(final WorkflowAction.Status status) {
        if (status == null) {
            return Color.BLACK;
        }

        final Color color;

        switch (status) {
            case PREP:
            case USER_RETRY:
            case START_RETRY:
            case START_MANUAL:
                color = Color.GREY;
                break;
            case RUNNING:
            case END_RETRY:
            case END_MANUAL:
                color = Color.YELLOW;
                break;
            case OK:
            case DONE:
                color = Color.GREEN;
                break;
            case ERROR:
            case FAILED:
            case KILLED:
                color = Color.RED;
                break;
            default:
                color = Color.BLACK;
        }

        return color;
    }

    private Node createOrGetGraphvizNode(final WorkflowActionNode node) {
        if (graphvizNodes.containsKey(node.getName())) {
            return graphvizNodes.get(node.getName());
        }

        addNode(node);

        return graphvizNodes.get(node.getName());
    }

    @Override
    public void addEdge(final WorkflowActionNode parent, final WorkflowActionNode child) {
        if (edges.containsEntry(parent.getName(), child.getName())) {
            return;
        }

        Node graphvizParent = createOrGetGraphvizNode(parent);

        graphvizParent = graphvizParent.link(
                Factory.to(createOrGetGraphvizNode(child)).with(calculateEdgeColor(child.getStatus())));
        graphvizNodes.put(parent.getName(), graphvizParent);

        edges.put(parent.getName(), child.getName());
        arcCount++;
    }

    private Color calculateEdgeColor(final WorkflowAction.Status childStatus) {
        if (childStatus == null) {
            return Color.BLACK;
        }

        if (childStatus.equals(WorkflowAction.Status.RUNNING)) {
            return Color.GREEN;
        }

        return getColor(childStatus);
    }

    @Override
    public void persist(final WorkflowActionNode node) {
        final Node graphvizNode = graphvizNodes.get(node.getName());
        graphvizGraph = graphvizGraph.with(graphvizNode);
    }

    @Override
    public BufferedImage renderPng() {
        final Future<BufferedImage> pngFuture = EXECUTOR_SERVICE.submit(new PngRenderer());

        try {
            return pngFuture.get(GRAPHVIZ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private int calculateHeight(final int arcCount) {
        return Math.min(arcCount * 100, 2000);
    }

    @Override
    public String renderDot() {
        return graphvizGraph.toString();
    }

    @Override
    public String renderSvg() {
        final Future<String> svgFuture = EXECUTOR_SERVICE.submit(new SvgRenderer());

        try {
            return svgFuture.get(GRAPHVIZ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private class PngRenderer implements Callable<BufferedImage> {
        @Override
        public BufferedImage call() throws Exception {
            final Graphviz graphviz = newGraphviz();

            return graphviz.render(Format.PNG).toImage();
        }
    }

    private class SvgRenderer implements Callable<String> {

        @Override
        public String call() throws Exception {
            final Graphviz graphviz = newGraphviz();

            return graphviz.render(Format.SVG).toString();
        }
    }

    private Graphviz newGraphviz() {
        // Defaults to Rasterizer#BATIK
        return Graphviz.fromGraph(graphvizGraph)
                .engine(Engine.DOT)
                .height(calculateHeight(arcCount));
    }
}