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
import com.google.gwt.cell.client.TextInputCell;
import com.google.gwt.xml.client.Text;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;
import com.orange.links.client.connection.Connection;
import com.orange.links.client.shapes.DecorationShape;

/**
 * Class for property table of decision node
 */
public class DecisionPropertyTable extends PropertyTable {

    private Grid gridName;
    private Grid gridForm;
    private Grid gridCase;
    private List<NodeWidget> neighbors;
    private ListBox addbox;
    private TextBox predbox;
    private CheckBox defaultcheck;
    private ListDataProvider<SwitchCase> dataProvider;
    private List<SwitchCase> cases;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public DecisionPropertyTable(NodeWidget w) {
        super(w);
        initConf();
        initWidget();
    }

    class SwitchCase {
        private NodeWidget widget;
        private String predicate;
        private Connection connection;

        public SwitchCase(NodeWidget w, String p, Connection c) {
            widget = w;
            predicate = p;
            connection = c;
        }

        public NodeWidget getWidget() {
            return widget;
        }

        public String getPreDicate() {
            return predicate;
        }

        public void setPredicate(String s) {
            predicate = s;
        }

        public Connection getConnection() {
            return connection;
        }

    }

    /**
     * Initialize configuration
     */
    private void initConf() {
        neighbors = new ArrayList<NodeWidget>();
        cases = new ArrayList<SwitchCase>();
        updateWidgetDropDown();
        updateNeighborList();
        dataProvider = new ListDataProvider<SwitchCase>(cases);
    }

    /**
     * Update a list of node widgets that this decision node has connection to
     */
    public void updateNeighborList() {

        List<SwitchCase> backup = new ArrayList<SwitchCase>();
        for (SwitchCase c : cases)
            backup.add(c);
        cases.clear();

        OozieDiagramController controller = current.getController();
        neighbors = controller.getCurrentNeighbor(current);
        if (neighbors == null) {
            neighbors = new ArrayList<NodeWidget>();
        }
        for (NodeWidget n : neighbors) {
            Connection conn = controller.getConnection(current, n);
            SwitchCase entry = null;

            // check existing case
            for (SwitchCase s : backup) {
                if (s.getWidget() == n) {
                    entry = s;
                    break;
                }
            }
            if (entry == null) {
                entry = new SwitchCase(n, "", conn);
            }
            cases.add(entry);
        }
    }

    /**
     * Update a table showing a list of switch cases
     */
    public void updateNeighborTable() {

        dataProvider = new ListDataProvider<SwitchCase>(cases);
        gridForm.setWidget(0, 1, createAddBox());
        gridForm.setWidget(0, 4, createAddButton());
        gridCase.setWidget(0, 1, createCaseTable());
    }

    private ListBox createAddBox() {
        addbox = new ListBox();
        for (int i = 0; i < widgetDropDown.size(); i++) {
            NodeWidget w = widgetDropDown.get(i);
            addbox.addItem(prettyItemString(w));
        }
        return addbox;
    }

    /**
     * Generate xml elements of decision node and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        Element action = doc.createElement("decision");
        action.setAttribute("name", name.getText());
        Element switchele = doc.createElement("switch");
        action.appendChild(switchele);
        for (SwitchCase ca : cases) {
            boolean isDefault = false;
            Connection conn = ca.getConnection();
            if (conn.getDecoration() != null) {
                isDefault = true;
            }
            if (isDefault) {
                Element defEle = doc.createElement("default");
                defEle.setAttribute("to", ca.getWidget().getName());
                switchele.appendChild(defEle);
            }
            else {
                Element caseEle = doc.createElement("case");
                caseEle.setAttribute("to", ca.getWidget().getName());
                Text txt = doc.createTextNode(ca.getPreDicate());
                caseEle.appendChild(txt);
                switchele.appendChild(caseEle);
            }
        }
        root.appendChild(action);
    }

    /**
     * Initialize widgets shown in property table
     */
    protected void initWidget() {

        this.setAlwaysShowScrollBars(true);
        this.setSize("100%", "80%");

        VerticalPanel container = new VerticalPanel();
        this.add(container);

        gridName = new Grid(1, 2);
        container.add(gridName);

        // Name Form
        name = insertTextRow(gridName, 0, "Name");

        // Button and TextBox to add new <case>
        gridForm = new Grid(1, 5);
        container.add(gridForm);
        gridForm.setWidget(0, 0, createLabel("Add New Case"));
        gridForm.setWidget(0, 1, createAddBox());
        predbox = new TextBox();
        gridForm.setWidget(0, 2, predbox);
        defaultcheck = new CheckBox("default");
        defaultcheck.setValue(false); // as default, <case to>
        // if <defaul> selected, make textbox for predicate read-only
        defaultcheck.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> event) {
                if (event.getValue()) {
                    predbox.setReadOnly(true);
                }
                else {
                    predbox.setReadOnly(false);
                }
            }
        });
        gridForm.setWidget(0, 3, defaultcheck);
        gridForm.setWidget(0, 4, createAddButton());

        // Table to store list of <case>
        gridCase = new Grid(1, 2);
        container.add(gridCase);
        gridCase.setWidget(0, 0, createLabel("Switch"));
        gridCase.setWidget(0, 1, createCaseTable());

    }

    /**
     * Create a button to add a new switch case
     *
     * @return
     */
    protected Button createAddButton() {
        Button btn = new Button("add path");
        btn.getElement().setAttribute("style",
                "margin:2px;padding:0px;-webkit-border-radius:5px;-moz-border-radius:5px;-border-radius:5px;");

        btn.addClickHandler(new ClickHandler() {

            @Override
            public void onClick(ClickEvent event) {
                OozieDiagramController controller = current.getController();
                NodeWidget target = widgetDropDown.get(addbox.getSelectedIndex());
                String predicate = predbox.getText();
                Boolean isDefault = defaultcheck.getValue();
                if (!controller.isConnected(current, target)) {
                    Connection conn = controller.addMultiConnection(current, target);
                    if (conn == null)
                        return;
                    // if default checked, add decoration label to connection
                    if (isDefault) {
                        initializeDefault(dataProvider.getList());
                        addDecorationDefaultLabel(conn);
                    }
                    SwitchCase newcase = new SwitchCase(target, predicate, conn);
                    dataProvider.getList().add(newcase);

                }
                else {
                    Window.alert("the case already exists!");
                }
            }
        });

        return btn;
    }

    /**
     * Create an expandable table to show switch cases
     *
     * @return
     */
    protected CellTable<SwitchCase> createCaseTable() {

        final CellTable<SwitchCase> table = new CellTable<SwitchCase>();
        dataProvider.addDataDisplay(table);

        // Add Case column
        Column<SwitchCase, String> caseCol = null;

        caseCol = new Column<SwitchCase, String>(new TextCell()) {
            @Override
            public String getValue(SwitchCase object) {
                return prettyItemString(object.getWidget());
            }
        };
        table.addColumn(caseCol, "To");

        // Add Predicate column
        Column<SwitchCase, String> predicateCol = null;

        predicateCol = new Column<SwitchCase, String>(new CustomTextCell()) {
            @Override
            public String getValue(SwitchCase object) {
                return object.getPreDicate();
            }
        };

        predicateCol.setFieldUpdater(new FieldUpdater<SwitchCase, String>() {
            @Override
            public void update(int index, SwitchCase object, String value) {
                object.setPredicate(value);
            }
        });
        table.addColumn(predicateCol, "Predicate");

        // Button to delete row
        Column<SwitchCase, String> defaultCol = new Column<SwitchCase, String>(new ButtonCell()) {
            @Override
            public String getValue(SwitchCase object) {
                Connection c = object.getConnection();
                DecorationShape ds = c.getDecoration();
                String rel = "Change to Default";
                if (ds != null)
                    rel = "Default";
                return rel;
            }
        };

        defaultCol.setFieldUpdater(new FieldUpdater<SwitchCase, String>() {

            @Override
            public void update(int index, SwitchCase object, String value) {
                initializeDefault(dataProvider.getList());
                Connection c = object.getConnection();
                addDecorationDefaultLabel(c);
                table.redraw();
            }
        });

        table.addColumn(defaultCol, "");

        // Button to delete row
        Column<SwitchCase, String> delCol = new Column<SwitchCase, String>(new ButtonCell()) {
            @Override
            public String getValue(SwitchCase object) {
                return " - ";
            }
        };

        delCol.setFieldUpdater(new FieldUpdater<SwitchCase, String>() {

            @Override
            public void update(int index, SwitchCase object, String value) {
                dataProvider.getList().remove(index);
                OozieDiagramController controller = (OozieDiagramController) current.getGenerator()
                        .getDiagramController();
                controller.removeConnection(current, object.getWidget());
            }
        });

        table.addColumn(delCol, "");

        return table;
    }

    class CustomTextCell extends TextInputCell {
        public void render(Context context, String data, SafeHtmlBuilder sb) {
            Connection c = ((SwitchCase) (context.getKey())).getConnection();
            if (c.getDecoration() != null) {
                sb.appendHtmlConstant("<div contentEditable='false' unselectable='true'>-----------</div>");
            }
            else {
                super.render(context, data, sb);
            }
        }
    }

    /**
     * Add a default label to a switch case
     *
     * @param c
     */
    public void addDecorationDefaultLabel(Connection c) {
        OozieDiagramController controller = (OozieDiagramController) current.getGenerator().getDiagramController();
        Label decorationLabel = new Label("Default");
        decorationLabel.getElement().setAttribute("style", "font-weight: bold;");
        controller.addDecoration(decorationLabel, c);
    }

    /**
     * Remove a default label from existing switch cases
     *
     * @param li
     */
    public void initializeDefault(List<SwitchCase> li) {

        OozieDiagramController controller = current.getController();
        for (SwitchCase s : li) {
            Connection c = s.getConnection();
            DecorationShape decoShape = c.getDecoration();
            if (decoShape != null) {
                controller.getView().remove(decoShape.asWidget());
            }
            c.removeDecoration();
        }
    }
}
