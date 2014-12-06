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

import edu.uci.ics.jung.algorithms.layout.StaticLayout;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.util.Context;
import edu.uci.ics.jung.visualization.VisualizationImageServer;
import edu.uci.ics.jung.visualization.renderers.Renderer;
import edu.uci.ics.jung.visualization.util.ArrowFactory;
import org.apache.commons.collections15.Transformer;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.client.WorkflowJob;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import javax.imageio.ImageIO;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class to generate and plot runtime workflow DAG
 */
public class GraphGenerator {

    private String xml;
    private WorkflowJobBean job;
    private boolean showKill = false;
    private final int actionsLimit = 25;

    /**
     * C'tor
     * @param xml The workflow definition XML
     * @param job Current status of the job
     * @param showKill Flag to whether show 'kill' node
     */
    public GraphGenerator(String xml, WorkflowJobBean job, boolean showKill) {
        if(job == null) {
            throw new IllegalArgumentException("JsonWorkflowJob can't be null");
        }
        this.xml = xml;
        this.job = job;
        this.showKill = showKill;
    }

    /**
     * C'tor
     * @param xml
     * @param job
     */
    public GraphGenerator(String xml, WorkflowJobBean job) {
        this(xml, job, false);
    }

    /**
     * Overridden to thwart finalizer attack
     */
    @Override
    public final void finalize() {
        // No-op; just to avoid finalizer attack
        // as the constructor is throwing an exception
    }

    /**
     * Stream the PNG file to client
     * @param out
     * @throws Exception
     */
    public void write(OutputStream out) throws Exception {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser saxParser = spf.newSAXParser();
        XMLReader xmlReader = saxParser.getXMLReader();
        xmlReader.setContentHandler(new XMLParser(out));
        xmlReader.parse(new InputSource(new StringReader(xml)));
    }

    private class XMLParser extends DefaultHandler {

        private OutputStream out;
        private LinkedHashMap<String, OozieWFNode> tags;

        private String action = null;
        private String actionOK = null;
        private String actionErr = null;
        private String actionType = null;
        private String fork;
        private String decision;

        public XMLParser(OutputStream out) {
            this.out = out;
        }

        @Override
        public void startDocument() throws SAXException {
            tags = new LinkedHashMap();
        }

        @Override
        public void endDocument() throws SAXException {

            if(tags.isEmpty()) {
                // Nothing to do here!
                return;
            }

            int maxX = Integer.MIN_VALUE;
            int maxY = Integer.MIN_VALUE;
            int minX = Integer.MAX_VALUE;
            int currX = 45;
            int currY = 45;
            final int xMargin = 205;
            final int yMargin = 50;
            final int xIncr = 215; // The widest element is 200 pixels (Rectangle)
            final int yIncr = 255; // The tallest element is 150 pixels; (Diamond)
            HashMap<String, WorkflowAction> actionMap = new HashMap<String, WorkflowAction>();

            // Create a hashmap for faster lookups
            // Also override showKill if there's any failed action
            boolean found = false;
            for(WorkflowAction wfAction : job.getActions()) {
                actionMap.put(wfAction.getName(), wfAction);
                if(!found) {
                    switch(wfAction.getStatus()) {
                        case KILLED:
                        case ERROR:
                        case FAILED:
                            showKill = true; // Assuming on error the workflow eventually ends with kill node
                            found = true;
                    }
                }
            }

            // Start building the graph
            DirectedSparseGraph<OozieWFNode, String> dg = new DirectedSparseGraph<OozieWFNode, String>();
            for(Map.Entry<String, OozieWFNode> entry : tags.entrySet()) {
                String name = entry.getKey();
                OozieWFNode node = entry.getValue();
                if(actionMap.containsKey(name)) {
                    node.setStatus(actionMap.get(name).getStatus());
                }

                // Set (x,y) coords of the vertices if not already set
                if(node.getLocation().equals(new Point(0, 0))) {
                    node.setLocation(currX, currY);
                }

                float childStep = showKill ? -(((float)node.getArcs().size() - 1 ) / 2)
                        : -((float)node.getArcs().size() / 2 - 1);
                int nodeX = node.getLocation().x;
                int nodeY = node.getLocation().y;
                for(Map.Entry<String, Boolean> arc : node.getArcs().entrySet()) {
                    if(!showKill && arc.getValue() && tags.get(arc.getKey()).getType().equals("kill")) {
                        // Don't show kill node (assumption: only error goes to kill node;
                        // No ok goes to kill node)
                        continue;
                    }
                    OozieWFNode child = tags.get(arc.getKey());
                    if(child == null) {
                        continue; // or throw error?
                    }
                    dg.addEdge(name + "-->" + arc.getKey(), node, child);
                    // TODO: Experimental -- should we set coords even if they're already set?
                    //if(child.getLocation().equals(new Point(0, 0))) {
                        int childX = (int)(nodeX + childStep * xIncr);
                        int childY = nodeY + yIncr;
                        child.setLocation(childX, childY);

                        if(minX > childX) {
                            minX = childX;
                        }
                        if(maxX < childX) {
                            maxX = childX;
                        }
                        if(maxY < childY) {
                            maxY = childY;
                        }
                    //}
                    childStep += 1;
                }

                currY += yIncr;
                currX = nodeX;
                if(minX > nodeX) {
                    minX = nodeX;
                }
                if(maxX < nodeX) {
                    maxX = nodeX;
                }
                if(maxY < nodeY) {
                    maxY = nodeY;
                }
            } // Done building graph

            final int padX = minX < 0 ? -minX: 0;

            Transformer<OozieWFNode, Point2D> locationInit = new Transformer<OozieWFNode, Point2D>() {

                @Override
                public Point2D transform(OozieWFNode node) {
                    if(padX == 0) {
                        return node.getLocation();
                    } else {
                        return new Point(node.getLocation().x + padX + xMargin, node.getLocation().y);
                    }
                }

            };

            StaticLayout<OozieWFNode, String> layout = new StaticLayout<OozieWFNode, String>(dg, locationInit, new Dimension(maxX + padX + xMargin, maxY));
            layout.lock(true);
            VisualizationImageServer<OozieWFNode, String> vis = new VisualizationImageServer<OozieWFNode, String>(layout, new Dimension(maxX + padX + 2 * xMargin, maxY + yMargin));

            vis.getRenderContext().setEdgeArrowTransformer(new ArrowShapeTransformer());
            vis.getRenderContext().setArrowDrawPaintTransformer(new ArcPaintTransformer());
            vis.getRenderContext().setEdgeDrawPaintTransformer(new ArcPaintTransformer());
            vis.getRenderContext().setEdgeStrokeTransformer(new ArcStrokeTransformer());
            vis.getRenderContext().setVertexShapeTransformer(new NodeShapeTransformer());
            vis.getRenderContext().setVertexFillPaintTransformer(new NodePaintTransformer());
            vis.getRenderContext().setVertexStrokeTransformer(new NodeStrokeTransformer());
            vis.getRenderContext().setVertexLabelTransformer(new NodeLabelTransformer());
            vis.getRenderContext().setVertexFontTransformer(new NodeFontTransformer());
            vis.getRenderer().getVertexLabelRenderer().setPosition(Renderer.VertexLabel.Position.CNTR);
            vis.setBackground(Color.WHITE);

            Dimension d = vis.getSize();
            BufferedImage img = new BufferedImage(d.width, d.height, BufferedImage.TYPE_INT_RGB);
            Graphics2D g = img.createGraphics();
            vis.paintAll(g);

            try {
                ImageIO.write(img, "png", out);
            }
            catch (IOException ioe) {
                throw new SAXException(ioe);
            }
            finally {
                try {
                    out.close(); //closing connection is imperative
                                 //regardless of ImageIO.write throwing exception or not
                                 //hence in finally block
                }
                catch (IOException e) {
                    XLog.getLog(getClass()).trace("Exception while closing OutputStream");
                }
                out = null;
                img.flush();
                g.dispose();
                vis.removeAll();
            }
        }

        @Override
        public void startElement(String namespaceURI,
                                String localName,
                                String qName,
                                Attributes atts)
            throws SAXException {
            if(localName.equalsIgnoreCase("start")) {
                String start = localName.toLowerCase();
                if(!tags.containsKey(start)) {
                    OozieWFNode v = new OozieWFNode(start, start);
                    v.addArc(atts.getValue("to"));
                    tags.put(start, v);
                }
            } else if(localName.equalsIgnoreCase("action")) {
                action = atts.getValue("name");
            } else if(action != null && actionType == null) {
                actionType = localName.toLowerCase();
            } else if(localName.equalsIgnoreCase("ok") && action != null && actionOK == null) {
                    actionOK = atts.getValue("to");
            } else if(localName.equalsIgnoreCase("error") && action != null && actionErr == null) {
                    actionErr = atts.getValue("to");
            } else if(localName.equalsIgnoreCase("fork")) {
                fork = atts.getValue("name");
                if(!tags.containsKey(fork)) {
                    tags.put(fork, new OozieWFNode(fork, localName.toLowerCase()));
                }
            } else if(localName.equalsIgnoreCase("path")) {
                tags.get(fork).addArc(atts.getValue("start"));
            } else if(localName.equalsIgnoreCase("join")) {
                String join = atts.getValue("name");
                if(!tags.containsKey(join)) {
                    OozieWFNode v = new OozieWFNode(join, localName.toLowerCase());
                    v.addArc(atts.getValue("to"));
                    tags.put(join, v);
                }
            } else if(localName.equalsIgnoreCase("decision")) {
                decision = atts.getValue("name");
                if(!tags.containsKey(decision)) {
                    tags.put(decision, new OozieWFNode(decision, localName.toLowerCase()));
                }
            } else if(localName.equalsIgnoreCase("case")
                    || localName.equalsIgnoreCase("default")) {
                tags.get(decision).addArc(atts.getValue("to"));
            } else if(localName.equalsIgnoreCase("kill")
                    || localName.equalsIgnoreCase("end")) {
                String name = atts.getValue("name");
                if(!tags.containsKey(name)) {
                    tags.put(name, new OozieWFNode(name, localName.toLowerCase()));
                }
            }
            if (tags.size() > actionsLimit) {
                tags.clear();
                throw new SAXException("Can't display the graph. Number of actions are more than display limit " + actionsLimit);
            }
        }

        @Override
        public void endElement(String namespaceURI,
                                String localName,
                                String qName)
                throws SAXException {
            if(localName.equalsIgnoreCase("action")) {
                tags.put(action, new OozieWFNode(action, actionType));
                tags.get(action).addArc(this.actionOK);
                tags.get(action).addArc(this.actionErr, true);
                action = null;
                actionOK = null;
                actionErr = null;
                actionType = null;
            }
        }

        private class OozieWFNode {
            private String name;
            private String type;
            private Point loc;
            private HashMap<String, Boolean> arcs;
            private Status status = null;

            public OozieWFNode(String name,
                    String type,
                    HashMap<String, Boolean> arcs,
                    Point loc,
                    Status status) {
                this.name = name;
                this.type = type;
                this.arcs = arcs;
                this.loc = loc;
                this.status = status;
            }

            public OozieWFNode(String name, String type, HashMap<String, Boolean> arcs) {
                this(name, type, arcs, new Point(0, 0), null);
            }

            public OozieWFNode(String name, String type) {
                this(name, type, new HashMap<String, Boolean>(), new Point(0, 0), null);
            }

            public OozieWFNode(String name, String type, WorkflowAction.Status status) {
                this(name, type, new HashMap<String, Boolean>(), new Point(0, 0), status);
            }

            public void addArc(String arc, boolean isError) {
                arcs.put(arc, isError);
            }

            public void addArc(String arc) {
                addArc(arc, false);
            }

            public void setName(String name) {
                this.name = name;
            }

            public void setType(String type) {
                this.type = type;
            }

            public void setLocation(Point loc) {
                this.loc = loc;
            }

            public void setLocation(double x, double y) {
                loc.setLocation(x, y);
            }

            public void setStatus(WorkflowAction.Status status) {
                this.status = status;
            }

            public String getName() {
                return name;
            }

            public String getType() {
                return type;
            }

            public HashMap<String, Boolean> getArcs() {
                return arcs;
            }

            public Point getLocation() {
                return loc;
            }

            public WorkflowAction.Status getStatus() {
                return status;
            }

            @Override
            public String toString() {
                StringBuilder s = new StringBuilder();

                s.append("Node: ").append(name).append("\t");
                s.append("Type: ").append(type).append("\t");
                s.append("Location: (").append(loc.getX()).append(", ").append(loc.getY()).append(")\t");
                s.append("Status: ").append(status).append("\n");
                Iterator<Map.Entry<String, Boolean>> it = arcs.entrySet().iterator();
                while(it.hasNext()) {
                    Map.Entry<String, Boolean> entry = it.next();

                    s.append("\t").append(entry.getKey());
                    if(entry.getValue().booleanValue()) {
                        s.append(" on error\n");
                    } else {
                        s.append("\n");
                    }
                }

                return s.toString();
            }
        }

        private class NodeFontTransformer implements Transformer<OozieWFNode, Font> {
            private final Font font = new Font("Default", Font.BOLD, 15);

            @Override
            public Font transform(OozieWFNode node) {
                return font;
            }
        }

        private class ArrowShapeTransformer implements Transformer<Context<Graph<OozieWFNode, String>, String>,  Shape> {
            private final Shape arrow = ArrowFactory.getWedgeArrow(10.0f, 20.0f);

            @Override
            public Shape transform(Context<Graph<OozieWFNode, String>, String> i) {
                return arrow;
            }
        }

        private class ArcPaintTransformer implements Transformer<String, Paint> {
            // Paint based on transition
            @Override
            public Paint transform(String arc) {
                int sep = arc.indexOf("-->");
                String source = arc.substring(0, sep);
                String target = arc.substring(sep + 3);
                OozieWFNode src = tags.get(source);
                OozieWFNode tgt = tags.get(target);

                if(src.getType().equals("start")) {
                    if(tgt.getStatus() == null) {
                        return Color.LIGHT_GRAY;
                    } else {
                        return Color.GREEN;
                    }
                }

                if(src.getArcs().get(target)) {
                    // Dealing with error transition (i.e. target is error)
                    if(src.getStatus() == null) {
                        return Color.LIGHT_GRAY;
                    }
                    switch(src.getStatus()) {
                        case KILLED:
                        case ERROR:
                        case FAILED:
                            return Color.RED;
                        default:
                            return Color.LIGHT_GRAY;
                    }
                } else {
                    // Non-error
                    if(src.getType().equals("decision")) {
                        // Check for target too
                        if(tgt.getStatus() != null) {
                            return Color.GREEN;
                        } else {
                            return Color.LIGHT_GRAY;
                        }
                    } else {
                        if(src.getStatus() == null) {
                            return Color.LIGHT_GRAY;
                        }
                        switch(src.getStatus()) {
                            case OK:
                            case DONE:
                            case END_RETRY:
                            case END_MANUAL:
                                return Color.GREEN;
                            default:
                                return Color.LIGHT_GRAY;
                        }
                    }
                }
            }
        }

        private class NodeStrokeTransformer implements Transformer<OozieWFNode, Stroke> {
            private final Stroke stroke1 = new BasicStroke(2.0f);
            private final Stroke stroke2 = new BasicStroke(4.0f);

            @Override
            public Stroke transform(OozieWFNode node) {
                if(node.getType().equals("start")
                        || node.getType().equals("end")
                        || node.getType().equals("kill")) {
                    return stroke2;
                }
                return stroke1;
            }
        }

        private class NodeLabelTransformer implements Transformer<OozieWFNode, String> {
            /*
            * 20 chars in rectangle in 2 rows max
            * 14 chars in diamond in 2 rows max
            * 9 in triangle in 2 rows max
            * 8 in invtriangle in 2 rows max
            * 8 in circle in 2 rows max
            */
            @Override
            public String transform(OozieWFNode node) {
                //return node.getType();
                String name = node.getName();
                String type = node.getType();
                StringBuilder s = new StringBuilder();
                if(type.equals("decision")) {
                    if(name.length() <= 14) {
                        return name;
                    } else {
                        s.append("<html>").append(name.substring(0, 12)).append("-<br />");
                        if(name.substring(13).length() > 14) {
                            s.append(name.substring(12, 25)).append("...");
                        } else {
                            s.append(name.substring(12));
                        }
                        s.append("</html>");
                        return s.toString();
                    }
                } else if(type.equals("fork")) {
                    if(name.length() <= 9) {
                        return "<html><br />" + name + "</html>";
                    } else {
                        s.append("<html><br />").append(name.substring(0, 7)).append("-<br />");
                        if(name.substring(8).length() > 9) {
                            s.append(name.substring(7, 15)).append("...");
                        } else {
                            s.append(name.substring(7));
                        }
                        s.append("</html>");
                        return s.toString();
                    }
                } else if(type.equals("join")) {
                    if(name.length() <= 8) {
                        return "<html>" + name + "</html>";
                    } else {
                        s.append("<html>").append(name.substring(0, 6)).append("-<br />");
                        if(name.substring(7).length() > 8) {
                            s.append(name.substring(6, 13)).append("...");
                        } else {
                            s.append(name.substring(6));
                        }
                        s.append("</html>");
                        return s.toString();
                    }
                } else if(type.equals("start")
                        || type.equals("end")
                        || type.equals("kill")) {
                    if(name.length() <= 8) {
                        return "<html>" + name + "</html>";
                    } else {
                        s.append("<html>").append(name.substring(0, 6)).append("-<br />");
                        if(name.substring(7).length() > 8) {
                            s.append(name.substring(6, 13)).append("...");
                        } else {
                            s.append(name.substring(6));
                        }
                        s.append("</html>");
                        return s.toString();
                    }
                }else {
                    if(name.length() <= 20) {
                        return name;
                    } else {
                        s.append("<html>").append(name.substring(0, 18)).append("-<br />");
                        if(name.substring(19).length() > 20) {
                            s.append(name.substring(18, 37)).append("...");
                        } else {
                            s.append(name.substring(18));
                        }
                        s.append("</html>");
                        return s.toString();
                    }
                }
            }
        }

        private class NodePaintTransformer implements Transformer<OozieWFNode, Paint> {
            @Override
            public Paint transform(OozieWFNode node) {
                WorkflowJob.Status jobStatus = job.getStatus();
                if(node.getType().equals("start")) {
                    return Color.WHITE;
                } else if(node.getType().equals("end")) {
                    if(jobStatus == WorkflowJob.Status.SUCCEEDED) {
                        return Color.GREEN;
                    }
                    return Color.BLACK;
                } else if(node.getType().equals("kill")) {
                    if(jobStatus == WorkflowJob.Status.FAILED
                            || jobStatus == WorkflowJob.Status.KILLED) {
                        return Color.RED;
                    }
                    return Color.WHITE;
                }

                // Paint based on status for rest
                WorkflowAction.Status status = node.getStatus();
                if(status == null) {
                    return Color.LIGHT_GRAY;
                }
                switch(status) {
                    case OK:
                    case DONE:
                    case END_RETRY:
                    case END_MANUAL:
                        return Color.GREEN;
                    case PREP:
                    case RUNNING:
                    case USER_RETRY:
                    case START_RETRY:
                    case START_MANUAL:
                        return Color.YELLOW;
                    case KILLED:
                    case ERROR:
                    case FAILED:
                        return Color.RED;
                    default:
                        return Color.LIGHT_GRAY;
                }
            }
        }

        private class NodeShapeTransformer implements Transformer<OozieWFNode, Shape> {
            private final Ellipse2D.Double circle = new Ellipse2D.Double(-40, -40, 80, 80);
            private final Rectangle rect = new Rectangle(-100, -30, 200, 60);
            private final Polygon diamond = new Polygon(new int[]{-75, 0, 75, 0}, new int[]{0, 75, 0, -75}, 4);
            private final Polygon triangle = new Polygon(new int[]{-85, 85, 0}, new int[]{0, 0, -148}, 3);
            private final Polygon invtriangle = new Polygon(new int[]{-85, 85, 0}, new int[]{0, 0, 148}, 3);

            @Override
            public Shape transform(OozieWFNode node) {
                if("start".equals(node.getType())
                    || "end".equals(node.getType())
                    || "kill".equals(node.getType())) {
                    return circle;
                }
                if("fork".equals(node.getType())) {
                    return triangle;
                }
                if("join".equals(node.getType())) {
                    return invtriangle;
                }
                if("decision".equals(node.getType())) {
                    return diamond;
                }
                return rect; // All action nodes
            }
        }

        private class ArcStrokeTransformer implements Transformer<String, Stroke> {
            private final Stroke stroke1 = new BasicStroke(2.0f);
            private final Stroke dashed = new BasicStroke(1.0f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f, new float[] {10.0f}, 0.0f);

            // Draw based on transition
            @Override
            public Stroke transform(String arc) {
                int sep = arc.indexOf("-->");
                String source = arc.substring(0, sep);
                String target = arc.substring(sep + 3);
                OozieWFNode src = tags.get(source);
                if(src.getArcs().get(target)) {
                        if(src.getStatus() == null) {
                            return dashed;
                        }
                        switch(src.getStatus()) {
                            case KILLED:
                            case ERROR:
                            case FAILED:
                                return stroke1;
                            default:
                                return dashed;
                        }
                } else {
                    return stroke1;
                }
            }
        }
    }
}
