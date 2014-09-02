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

package org.apache.oozie.tools.workflowgenerator.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.oozie.tools.workflowgenerator.client.property.control.WrkflowPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.KillNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.StartNodeWidget;

import com.google.gwt.dom.client.Style.Cursor;
import com.google.gwt.user.client.Command;
import com.google.gwt.user.client.ui.MenuItem;
import com.google.gwt.user.client.ui.Panel;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Node;
import com.google.gwt.xml.client.XMLParser;
import com.orange.links.client.DiagramController;
import com.orange.links.client.connection.Connection;
import com.orange.links.client.event.TieLinkEvent;
import com.orange.links.client.menu.ContextMenu;
import com.orange.links.client.shapes.FunctionShape;

/**
 * Class to provide a panel where users create node widget or draw a line to
 * construct a workflow
 */
public class OozieDiagramController extends DiagramController {

    private Panel xmlpanel;
    private WrkflowPropertyTable wrkflowtable;

    /**
     * Constructor which records canvas size
     *
     * @param canvasWidth width of canvas
     * @param canvasHeight height of canvas
     */
    public OozieDiagramController(int canvasWidth, int canvasHeight) {
        super(canvasWidth, canvasHeight);
    }

    /**
     * Constructor which records canvas size and frame size
     *
     * @param canvasWidth width of canvas
     * @param canvasHeight height of canvas
     * @param frameWidth width of frame
     * @param frameHeight height of frame
     */
    public OozieDiagramController(int canvasWidth, int canvasHeight, int frameWidth, int frameHeight) {
        super(canvasWidth, canvasHeight, frameWidth, frameHeight);
    }

    /**
     * Initialize right-click context menu
     */
    public void initMenu() {
        canvasMenu = new ContextMenu();
        canvasMenu.addItem(new MenuItem("Generate XML", new Command() {
            @Override
            public void execute() {
                generateXml();
            }
        }));

    }

    /**
     * Return a list of neighboring node widgets which the specified node widget
     * has connection to
     *
     * @param w node widget
     * @return
     */
    public List<NodeWidget> getCurrentNeighbor(NodeWidget w) {
        List<NodeWidget> current = new ArrayList<NodeWidget>();
        FunctionShape s = widgetShapeMap.get(w);
        Map<Widget, Connection> connectMap = functionsMap.get(w);
        if (connectMap == null) {
            return null;
        }
        for (Iterator<Map.Entry<Widget, Connection>> it = connectMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Widget, Connection> entry = it.next();
            Connection c = entry.getValue();
            if (c.getStartShape() == s) {
                FunctionShape e = (FunctionShape) c.getEndShape();
                current.add((NodeWidget) e.asWidget());
            }
        }

        return current;
    }

    /**
     * Return a list of connections that the specified node widget has
     *
     * @param w node widget
     * @return
     */
    public List<Connection> getConnection(NodeWidget w) {
        List<Connection> li = new ArrayList<Connection>();
        FunctionShape shape = widgetShapeMap.get(w);
        for (Connection c : shape.getConnections()) {
            li.add(c);
        }
        return li;
    }

    /**
     * Add a new connection from one node widget to the other, while deleting
     * existing connection that the start node widget has if any
     *
     * @param start start node widget
     * @param end end node widget
     */
    public void addConnection(NodeWidget start, NodeWidget end) {
        boolean exists = false;
        FunctionShape s1 = widgetShapeMap.get(start);
        FunctionShape s2 = widgetShapeMap.get(end);
        Connection current = null;
        for (Connection c : s1.getConnections()) {
            if (c.getStartShape() == s1) {
                current = c;
            }
            if (c.getEndShape() == s2) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            if (current != null) {
                this.deleteConnection(current);
                s1.removeConnection(current);
                s2.removeConnection(current);
            }
            Connection c = drawStraightArrowConnection(start, end);
            fireEvent(new TieLinkEvent(start, end, c));
        }
    }

    /**
     * Add a new connection from one node widget to the other without deleting
     * existing connection that the starting node widget has
     *
     * @param start start node widget
     * @param end end node widget
     * @return
     */
    public Connection addMultiConnection(NodeWidget start, NodeWidget end) {
        boolean exists = false;
        FunctionShape s1 = widgetShapeMap.get(start);
        FunctionShape s2 = widgetShapeMap.get(end);
        Connection conn = null;

        for (Connection c : s1.getConnections()) {
            if (c.getEndShape() == s2) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            conn = drawStraightArrowConnection(start, end);
            fireEvent(new TieLinkEvent(start, end, conn));
        }

        return conn;
    }

    /**
     * Return a connection between node widgets
     *
     * @param start start node widget
     * @param end end node widget
     * @return
     */
    public Connection getConnection(NodeWidget start, NodeWidget end) {

        Connection conn = null;
        Map<Widget, Connection> m = (Map<Widget, Connection>) functionsMap.get(start);
        if (m != null) {
            conn = (Connection) m.get(end);
        }
        return conn;
    }

    /**
     * Return if there is a connection between node widgets
     *
     * @param start start node widget
     * @param end end node widget
     * @return
     */
    public boolean isConnected(NodeWidget start, NodeWidget end) {
        return functionsMap.get(start).containsKey(end);
    }

    /**
     * Remove a connection between node widgets
     *
     * @param start start node widget
     * @param end end node widget
     */
    public void removeConnection(NodeWidget start, NodeWidget end) {

        FunctionShape s1 = widgetShapeMap.get(start);
        FunctionShape s2 = widgetShapeMap.get(end);

        if (functionsMap.get(start).containsKey(end)) {
            Connection c = functionsMap.get(start).get(end);
            this.deleteConnection(c);
            s1.removeConnection(c);
            s2.removeConnection(c);
            functionsMap.get(start).remove(end);
        }
    }

    /**
     * Set a panel to display generated xml
     *
     * @param w
     */
    public void setXmlPanel(Panel w) {
        this.xmlpanel = w;
    }

    /**
     * Generate a xml document of the current workflow
     */
    public void generateXml() {

        Document xmldoc = null;
        xmldoc = (Document) XMLParser.createDocument();

        // initialize visit-flag of all widgets
        for (Widget w : widgetShapeMap.keySet()) {
            ((NodeWidget) w).unvisit();
        }

        NodeWidget current = null;
        NodeWidget next = null;
        LinkedList<NodeWidget> queue = new LinkedList<NodeWidget>();

        String appname = "visualizationApp";
        if (wrkflowtable != null) {
            appname = wrkflowtable.getName();
        }
        Element root = xmldoc.createElement("workflow-app");
        root.setAttribute("xmlns", wrkflowtable.getNameSpace());
        root.setAttribute("name", appname);
        xmldoc.appendChild(root);

        wrkflowtable.generateXML(xmldoc, root, null);

        for (Connection c : this.connections) {
            FunctionShape startShape = (FunctionShape) c.getStartShape();
            NodeWidget w = (NodeWidget) startShape.asWidget();
            if (w instanceof StartNodeWidget) {
                queue.add(w);
                break;
            }
        }

        while ((current = queue.poll()) != null) {

            if (current.isVisited()) {
                continue;
            }
            Map<Widget, Connection> nextMap = this.functionsMap.get(current);
            if (nextMap == null) {
                next = null;
            }
            else {
                for (Iterator itr = nextMap.keySet().iterator(); itr.hasNext();) {
                    next = (NodeWidget) itr.next();
                    queue.add(next);
                }
            }
            current.generateXML(xmldoc, root, next);
            current.visit();
        }

        // Adding Kill Node
        for (Widget w : this.widgetShapeMap.keySet()) {
            if (w instanceof KillNodeWidget) {
                ((KillNodeWidget) w).generateXML(xmldoc, root, null);
            }
        }

        xmlpanel.clear();
        TextArea xml = new TextArea();
        xml.getElement().setScrollTop(xml.getElement().getScrollHeight());
        xml.setCharacterWidth(80);
        xml.setVisibleLines(30);
        xml.setText(formatXML(root, "  "));
        xmlpanel.add(xml);

    }

    /**
     * Format generated xml doc by indentation
     *
     * @param node
     * @param indent
     * @return
     */
    protected String formatXML(Node node, String indent) {
        StringBuilder formatted = new StringBuilder();

        if (node.getNodeType() == Node.ELEMENT_NODE) {
            StringBuilder attributes = new StringBuilder();
            for (int k = 0; k < node.getAttributes().getLength(); k++) {
                attributes.append(" ");
                attributes.append(node.getAttributes().item(k).getNodeName());
                attributes.append("=\"");
                attributes.append(node.getAttributes().item(k).getNodeValue());
                attributes.append("\"");
            }

            formatted.append(indent);
            formatted.append("<");
            formatted.append(node.getNodeName());
            formatted.append(attributes.toString());
            if (!node.hasChildNodes() || (node.hasChildNodes() && node.getFirstChild().getNodeType() == Node.TEXT_NODE)) {
                formatted.append(">");
            }
            else {
                formatted.append(">\n");
            }

            for (int i = 0; i < node.getChildNodes().getLength(); i++) {
                formatted.append(formatXML(node.getChildNodes().item(i), indent + "   "));
            }

            if (node.hasChildNodes() && node.getFirstChild().getNodeType() != Node.TEXT_NODE) {
                formatted.append(indent);
            }
            formatted.append("</");
            formatted.append(node.getNodeName());
            formatted.append(">\n");
        }
        else {
            if (node.toString().trim().length() > 0) {
                formatted.append(node.toString());
            }
        }

        return formatted.toString();
    }

    /**
     * Update a diagram controller
     */
    @Override
    public void update() {
        redrawConnections();

        // If the user is dragging widgets, do nothing
        if (inDragWidget) {
            return;
        }
        topCanvas.clear();

        // Search for selectable area
        if (!inDragBuildArrow) {
            for (FunctionShape s : shapes) {
                if (s.isMouseNearSelectableArea(mousePoint)) {
                    s.highlightSelectableArea(mousePoint);
                    inEditionSelectableShapeToDrawConnection = true;
                    startFunctionWidget = s.asWidget();
                    RootPanel.getBodyElement().getStyle().setCursor(Cursor.POINTER);
                    return;
                }
                inEditionSelectableShapeToDrawConnection = false;
            }
        }
        else {
            // Don't go deeper if in edition mode
            // If mouse over a widget, highlight it
            FunctionShape s = getShapeUnderMouse();
            if (s != null) {
                s.drawHighlight();
                highlightFunctionShape = s;
            }
            else if (highlightFunctionShape != null) {
                highlightFunctionShape.draw();
                highlightFunctionShape = null;
            }
            clearAnimationsOnCanvas();
        }

        // Test if in Drag Movable Point
        if (!inDragMovablePoint && !inDragBuildArrow) {
            for (Connection c : connections) {
                if (c.isMouseNearConnection(mousePoint)) {
                    highlightPoint = c.highlightMovablePoint(mousePoint);
                    highlightConnection = getConnectionNearMouse();
                    inEditionDragMovablePoint = true;
                    RootPanel.getBodyElement().getStyle().setCursor(Cursor.POINTER);
                    return;
                }
                inEditionDragMovablePoint = false;
            }
        }

        clearAnimationsOnCanvas();
    }

    /**
     * Return a workflow property table
     *
     * @return
     */
    public WrkflowPropertyTable getWrkflowPropertyTable() {
        return wrkflowtable;
    }

    /**
     * Set a workflow property table
     *
     * @param t Workflow property table
     */
    public void setWrkflowPropertyTable(WrkflowPropertyTable t) {
        this.wrkflowtable = t;
    }

}
