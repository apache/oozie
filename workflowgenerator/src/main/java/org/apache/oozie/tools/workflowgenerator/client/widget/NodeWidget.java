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

package org.apache.oozie.tools.workflowgenerator.client.widget;

import org.apache.oozie.tools.workflowgenerator.client.OozieDiagramController;
import org.apache.oozie.tools.workflowgenerator.client.OozieWorkflowGenerator;
import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTableFactory;

import com.google.gwt.event.dom.client.HasAllTouchHandlers;
import com.google.gwt.event.dom.client.MouseDownEvent;
import com.google.gwt.event.dom.client.MouseDownHandler;
import com.google.gwt.event.dom.client.TouchCancelEvent;
import com.google.gwt.event.dom.client.TouchCancelHandler;
import com.google.gwt.event.dom.client.TouchEndEvent;
import com.google.gwt.event.dom.client.TouchEndHandler;
import com.google.gwt.event.dom.client.TouchMoveEvent;
import com.google.gwt.event.dom.client.TouchMoveHandler;
import com.google.gwt.event.dom.client.TouchStartEvent;
import com.google.gwt.event.dom.client.TouchStartHandler;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.user.client.Command;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.MenuItem;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;
import com.orange.links.client.menu.ContextMenu;
import com.orange.links.client.menu.HasContextMenu;
import com.orange.links.client.save.IsDiagramSerializable;

/**
 * Abstract class used as base for node widget
 */
public abstract class NodeWidget extends Label implements HasContextMenu, HasAllTouchHandlers, IsDiagramSerializable {

    protected ContextMenu contextMenu;
    protected OozieWorkflowGenerator generator;
    protected OozieDiagramController controller;
    protected PropertyTable table;
    protected boolean visited = false;
    protected String identifier = "nodewidget"; // for serialization to be
                                                // implemented
    protected String content = ""; // for serialization to be implemented

    public NodeWidget() {
        super("");
        initMenu();
        initEvent();
    }

    /**
     * Constructor which records OozieWorkflowGenerator
     *
     * @param gen OozieWorkflowGenerator
     */
    public NodeWidget(OozieWorkflowGenerator gen, String style) {
        this();
        generator = gen;
        controller = gen.getDiagramController();
        setStyleName(style);
        initPropertyTable();
    }

    /**
     * Initialize property table
     */
    protected void initPropertyTable() {
        PropertyTableFactory factory = PropertyTableFactory.getInstance();
        table = factory.createPropertyTable(this);
        generator.setPropertyTable(table);
    }

    /**
     * Initialize right-click context menu
     */
    protected void initMenu() {
        contextMenu = new ContextMenu();
        contextMenu.addItem(new MenuItem("Delete", new Command() {
            @Override
            public void execute() {
                generator.removeWidget(NodeWidget.this);
            }
        }));
    }

    /**
     * Initialize event handler
     */
    protected void initEvent() {

        // Register MouseDown Event to update property table of the selected
        // action
        this.addMouseDownHandler(new MouseDownHandler() {

            @Override
            public void onMouseDown(MouseDownEvent event) {
                generator.setPropertyTable(table);
                updateOnSelection();
            }
        });
    }

    /**
     * Update information when clicked in workflow design panel
     */
    protected abstract void updateOnSelection();

    /**
     * Return an instance of OozieWorkflowGenerator
     *
     * @return
     */
    public OozieWorkflowGenerator getGenerator() {
        return this.generator;
    }

    /**
     * Return an instance of property table
     *
     * @return Property Tab
     */
    public PropertyTable getPropertyTable() {
        return this.table;
    }

    /**
     * Return an instance of OozieDiagramController
     *
     * @return
     */
    public OozieDiagramController getController() {
        return this.controller;
    }

    /**
     * Return a name of node widget
     *
     * @return
     */
    public String getName() {
        return table.getName();
    }

    /**
     * Set a name of node widget
     *
     * @param n name
     */
    public void setName(String n) {
        table.setName(n);
    }

    /**
     * Generate xml elements of this node widget and attach them to xml doc
     *
     * @param doc xml document
     * @param root xml element under which elements of this node widget are
     *        added
     * @param next next node widget to be executed after this in workflow
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {
        table.generateXML(doc, root, next);
    }

    /**
     * Add TouchStartHandler
     */
    @Override
    public HandlerRegistration addTouchStartHandler(TouchStartHandler handler) {
        return addDomHandler(handler, TouchStartEvent.getType());
    }

    /**
     * Add TouchEndHandler
     */
    @Override
    public HandlerRegistration addTouchEndHandler(TouchEndHandler handler) {
        return addDomHandler(handler, TouchEndEvent.getType());
    }

    /**
     * Add TouchMoveHandler
     */
    @Override
    public HandlerRegistration addTouchMoveHandler(TouchMoveHandler handler) {
        return addDomHandler(handler, TouchMoveEvent.getType());
    }

    /**
     * Add TouchCancelHandler
     */
    @Override
    public HandlerRegistration addTouchCancelHandler(TouchCancelHandler handler) {
        return addDomHandler(handler, TouchCancelEvent.getType());
    }

    /**
     * Return a type of node widget (used for serialization not yet implemented)
     */
    @Override
    public String getType() {
        return this.identifier;
    }

    /**
     * Return content representation of node widget (used for serialization not
     * yet implemented)
     */
    @Override
    public String getContentRepresentation() {
        return this.content;
    }

    /**
     * Set content representation of node widget (used for serialization not yet
     * implemented)
     */
    @Override
    public void setContentRepresentation(String contentRepresentation) {
        this.content = contentRepresentation;
    }

    /**
     * Return right-click context menu
     */
    @Override
    public ContextMenu getContextMenu() {
        return this.contextMenu;
    }

    /**
     * Return if node widget is already visited in tree-traversal algorithm
     * which generates xml
     *
     * @return
     */
    public boolean isVisited() {
        return this.visited;
    }

    /**
     * Set flag when node widget is visited in tree-traversal algorithm which
     * generates xml
     */
    public void visit() {
        this.visited = true;
    }

    /**
     * Initialize visited flag
     */
    public void unvisit() {
        this.visited = false;
    }

}
