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

package org.apache.oozie.tools.workflowgenerator.client.property.control;

import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.tools.workflowgenerator.client.OozieDiagramController;
import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

import com.google.gwt.cell.client.ButtonCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.cell.client.TextCell;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of fork node
 */
public class ForkPropertyTable extends PropertyTable {

    private Grid gridName;
    private Grid gridForm;
    private Grid gridPath;
    private List<NodeWidget> neighbors;
    private ListBox addbox;
    private ListDataProvider<NodeWidget> dataProvider;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public ForkPropertyTable(NodeWidget w) {
        super(w);
        initConf();
        initWidget();
    }

    /**
     * Initialize configuration
     */
    private void initConf() {
        neighbors = new ArrayList<NodeWidget>();
        updateWidgetDropDown();
        updateNeighborList();
    }

    /**
     * Update a list of node widgets that this fork node has connection to
     */
    public void updateNeighborList() {

        OozieDiagramController controller = current.getController();
        neighbors = controller.getCurrentNeighbor(current);
        if (neighbors == null) {
            neighbors = new ArrayList<NodeWidget>();
        }
        dataProvider = new ListDataProvider<NodeWidget>(neighbors);
    }

    /**
     * Update a table listing node widgets that this fork has connection to
     */
    public void updateNeighborTable() {
        gridForm.setWidget(0, 1, createAddBox());
        gridForm.setWidget(0, 2, createAddButton());
        gridPath.setWidget(0, 1, createPathTable());
    }

    /**
     * Create dropdown list of node widgets to create a new path
     *
     * @return
     */
    private ListBox createAddBox() {
        addbox = new ListBox();
        for (int i = 0; i < widgetDropDown.size(); i++) {
            NodeWidget w = widgetDropDown.get(i);
            addbox.addItem(prettyItemString(w));
        }
        return addbox;
    }

    /**
     * Generate xml elements of fork node and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        Element action = doc.createElement("fork");
        action.setAttribute("name", name.getText());
        for (NodeWidget w : neighbors) {
            Element pathEle = doc.createElement("path");
            pathEle.setAttribute("start", w.getName());
            action.appendChild(pathEle);
        }
        root.appendChild(action);

    }

    /**
     * Initialize widgets shown in property table
     */
    protected void initWidget() {

        VerticalPanel vertical = new VerticalPanel();
        this.add(vertical);

        this.setAlwaysShowScrollBars(true);
        this.setSize("100%", "80%");

        // insert row for Name
        gridName = new Grid(1, 2);
        vertical.add(gridName);
        name = insertTextRow(gridName, 0, "Name");

        // insert row for Add Path Input/Button
        gridForm = new Grid(1, 3);
        vertical.add(gridForm);
        gridForm.setWidget(0, 0, createLabel("Add new path"));
        gridForm.setWidget(0, 1, createAddBox());
        gridForm.setWidget(0, 2, createAddButton());

        // insert row for Fork Path List
        gridPath = new Grid(1, 2);
        vertical.add(gridPath);
        gridPath.setWidget(0, 0, createLabel("Path List"));
        gridPath.setWidget(0, 1, createPathTable());
    }

    /**
     * Create a button to add a new path
     *
     * @return
     */
    protected Button createAddButton() {
        Button btn = new Button("add");
        btn.getElement().setAttribute("style",
                "margin:2px;padding:0px;-webkit-border-radius:5px;-moz-border-radius:5px;-border-radius:5px;");

        btn.addClickHandler(new ClickHandler() {

            @Override
            public void onClick(ClickEvent event) {
                OozieDiagramController controller = (OozieDiagramController) current.getGenerator()
                        .getDiagramController();
                NodeWidget w = widgetDropDown.get(addbox.getSelectedIndex());
                if (!neighbors.contains(w)) {
                    dataProvider.getList().add(w);
                    controller.addMultiConnection(current, w);
                }
                else {
                    Window.alert("the path already exists!");
                }
            }
        });

        return btn;
    }

    /**
     * Create a expandable table listing paths that this fork node has
     *
     * @return
     */
    protected CellTable<NodeWidget> createPathTable() {

        CellTable<NodeWidget> table = new CellTable<NodeWidget>();
        dataProvider.addDataDisplay(table);

        // Add Name column
        Column<NodeWidget, String> nameCol = null;

        nameCol = new Column<NodeWidget, String>(new TextCell()) {
            @Override
            public String getValue(NodeWidget object) {
                return prettyItemString(object);
            }
        };
        table.addColumn(nameCol, "To");

        // Button to delete row
        Column<NodeWidget, String> delCol = new Column<NodeWidget, String>(new ButtonCell()) {
            @Override
            public String getValue(NodeWidget object) {
                return " - ";
            }
        };

        delCol.setFieldUpdater(new FieldUpdater<NodeWidget, String>() {

            @Override
            public void update(int index, NodeWidget object, String value) {
                List<NodeWidget> li = (List<NodeWidget>) dataProvider.getList();
                dataProvider.getList().remove(index);
                OozieDiagramController controller = current.getController();
                controller.removeConnection(current, object);
            }
        });

        table.addColumn(delCol, "");

        return table;
    }

    protected void display() {
        this.clear();

    }
}
