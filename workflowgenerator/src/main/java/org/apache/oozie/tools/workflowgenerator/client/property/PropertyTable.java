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

package org.apache.oozie.tools.workflowgenerator.client.property;

import java.util.ArrayList;
import java.util.List;
import org.apache.oozie.tools.workflowgenerator.client.OozieDiagramController;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.EmailActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.FSActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.JavaActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.MapReduceActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.PigActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.PipesActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.SSHActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.ShellActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.StreamingActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.SubWFActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.DecisionNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.EndNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.ForkNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.JoinNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.KillNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.StartNodeWidget;
import com.google.gwt.cell.client.ButtonCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.cell.client.SelectionCell;
import com.google.gwt.cell.client.TextInputCell;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.ScrollPanel;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Text;

/**
 * Base abstract class for property table;
 */
public abstract class PropertyTable extends ScrollPanel {

    protected NodeWidget current;
    protected TextBox name;
    protected ListBox okVal;
    protected ListBox errorVal;
    protected TextBox jt;
    protected TextBox nn;
    protected List<NodeWidget> widgetDropDown;
    protected HandlerRegistration handler;
    protected Grid grid;

    /**
     * Constructor which records node widget
     *
     * @param w node widget
     */
    public PropertyTable(NodeWidget w) {
        super();
        this.current = w;
    }

    /**
     * Update a list of other node widgets shown in "OK" drop-down, excluding
     * start and kill nodes
     */
    public void updateWidgetDropDown() {

        List<NodeWidget> widgetList = current.getGenerator().getWidgets();
        OozieDiagramController controller = current.getController();

        if (widgetDropDown == null) {
            widgetDropDown = new ArrayList<NodeWidget>();
        }
        else {
            widgetDropDown.clear();
        }

        if (widgetList != null) {
            for (NodeWidget w : widgetList) {
                if (!(w instanceof KillNodeWidget) && !(w instanceof StartNodeWidget) && w != current) {
                    widgetDropDown.add(w);
                }
            }
        }

        // if okVal listbox doesn't exist, return
        if (okVal != null) {
            okVal.clear();
        }
        else {
            return;
        }
        // insert empty option on top
        okVal.addItem("");

        for (int i = 0; i < widgetDropDown.size(); i++) {
            NodeWidget w = widgetDropDown.get(i);
            okVal.addItem(prettyItemString(w));
            // option selected when this node widget is connected to the
            // widget in a design panel
            List<NodeWidget> neigh = controller.getCurrentNeighbor(current);
            if (neigh != null && neigh.size() > 0 && w == neigh.get(0))
                okVal.setSelectedIndex(i + 1);
        }

        // remove previous handler, otherwise, end up having multiple handlers
        if (handler != null) {
            handler.removeHandler();
        }

        handler = okVal.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                int selectedIndex = okVal.getSelectedIndex();
                if (selectedIndex > 0) {
                    NodeWidget target = widgetDropDown.get(selectedIndex - 1);
                    current.getController().addConnection(current, target);
                }

            }
        });
    }

    /**
     * Update "Error" drop-down list, currently only includes default kill node
     */
    public void updateErrorDropDown() {

        List<NodeWidget> widgetList = current.getGenerator().getWidgets();

        if (errorVal == null) {
            errorVal = new ListBox();
        }
        else {
            errorVal.clear();
        }

        for (NodeWidget w : widgetList) {
            if (w instanceof KillNodeWidget) {
                errorVal.addItem(prettyItemString(w));
                errorVal.setSelectedIndex(0);
            }
        }
    }

    /**
     * Return an instance of a kill node
     *
     * @return
     */
    protected NodeWidget getKillNode() {

        List<NodeWidget> widgetList = current.getGenerator().getWidgets();
        NodeWidget node = null;
        for (NodeWidget w : widgetList) {
            if (w instanceof KillNodeWidget)
                node = w;
        }
        return node;
    }

    /**
     * Abstract method to generate xml elements and attach them to xml doc
     *
     * @param doc xml document
     * @param root xml element under which generated xml elements are added
     * @param next next node widget to be executed after this in workflow
     */
    public abstract void generateXML(Document doc, Element root, NodeWidget next);

    /**
     * Append a type of the node widget to its name for readability
     *
     * @param w
     * @return
     */
    protected String prettyItemString(NodeWidget w) {
        StringBuilder s = new StringBuilder();
        s.append(w.getName());
        s.append(" ");
        if (w instanceof EndNodeWidget) {
            s.append(" (End Node)");
        }
        else if (w instanceof ForkNodeWidget) {
            s.append(" (Fork Node)");
        }
        else if (w instanceof JoinNodeWidget) {
            s.append(" (Join Node)");
        }
        else if (w instanceof MapReduceActionWidget) {
            s.append(" (MR Action)");
        }
        else if (w instanceof PigActionWidget) {
            s.append(" (Pig Action)");
        }
        else if (w instanceof StreamingActionWidget) {
            s.append(" (MR Streaming Action)");
        }
        else if (w instanceof PipesActionWidget) {
            s.append(" (MR Pipes Action)");
        }
        else if (w instanceof JavaActionWidget) {
            s.append(" (Java Action)");
        }
        else if (w instanceof FSActionWidget) {
            s.append(" (FS Action)");
        }
        else if (w instanceof DecisionNodeWidget) {
            s.append(" (Decision Node)");
        }
        else if (w instanceof KillNodeWidget) {
            s.append(" (Kill Node)");
        }
        else if (w instanceof SSHActionWidget) {
            s.append(" (SSH Node)");
        }
        else if (w instanceof SubWFActionWidget) {
            s.append(" (SubWF Node)");
        }
        else if (w instanceof EmailActionWidget) {
            s.append(" (Email Node)");
        }
        else if (w instanceof ShellActionWidget) {
            s.append(" (Shell Node)");
        }
        return s.toString();
    }

    /**
     * Set the width of widget
     *
     * @param w widget
     * @return
     */
    protected Widget formatCell(Widget w) {
        w.setWidth("300px");
        return w;
    }

    /**
     * Create an expandable sub table as a part of property table
     *
     * @param colname1 1st column name
     * @param colname2 2nd column name
     * @param data data list
     * @param options listbox options, if null, text input cell used
     * @return
     */
    protected CellTable<Property> createSubTable(String colname1, String colname2, List<Property> data,
            List<String> options) {

        final CellTable<Property> table = new CellTable<Property>();
        final ListDataProvider<Property> dataProvider = new ListDataProvider<Property>();
        dataProvider.setList(data);
        dataProvider.addDataDisplay(table);

        // add Name column
        Column<Property, String> nameCol = null;

        if (options == null) {
            nameCol = new Column<Property, String>(new TextInputCell()) {
                @Override
                public String getValue(Property object) {
                    return object.getName();
                }
            };
        }
        else {
            nameCol = new Column<Property, String>(new SelectionCell(options)) {
                @Override
                public String getValue(Property object) {
                    return object.getName();
                }
            };
        }

        // set event for updating value
        nameCol.setFieldUpdater(new FieldUpdater<Property, String>() {
            @Override
            public void update(int index, Property object, String value) {
                object.setName(value);
            }
        });
        table.addColumn(nameCol, colname1);

        // Add Value column
        Column<Property, String> valueCol = new Column<Property, String>(new TextInputCell()) {
            @Override
            public String getValue(Property object) {
                return object.getValue();
            }
        };

        valueCol.setFieldUpdater(new FieldUpdater<Property, String>() {
            @Override
            public void update(int index, Property object, String value) {
                object.setValue(value);
            }
        });
        table.addColumn(valueCol, colname2);

        // Button to add row
        Column<Property, String> addCol = new Column<Property, String>(new ButtonCell()) {
            @Override
            public String getValue(Property object) {
                return " + ";
            }
        };
        addCol.setFieldUpdater(new FieldUpdater<Property, String>() {
            @Override
            public void update(int index, Property object, String value) {
                dataProvider.getList().add(index + 1, new Property("", ""));
            }
        });

        table.addColumn(addCol, "");

        // Button to delete row
        Column<Property, String> delCol = new Column<Property, String>(new ButtonCell()) {
            @Override
            public String getValue(Property object) {
                return " - ";
            }
        };

        delCol.setFieldUpdater(new FieldUpdater<Property, String>() {

            @Override
            public void update(int index, Property object, String value) {
                List<Property> li = dataProvider.getList();
                if (li.size() == 1) {
                    Property p = li.get(0);
                    p.setName("");
                    p.setValue("");
                    table.redraw();
                }
                else
                    dataProvider.getList().remove(index);
            }
        });

        table.addColumn(delCol, "");

        return table;
    }

    /**
     * Create an add button in a table
     *
     * @param table
     * @return
     */
    protected Button createAddButton(Grid table) {
        Button btn = new Button("+");
        btn.getElement()
                .setAttribute("style",
                        "font-size:20px;margin:0px;padding:0px;-webkit-border-radius:10px;-moz-border-radius:10px;-border-radius:10px;");
        return btn;
    }

    /**
     * create a delete button in a table
     *
     * @param table
     * @return
     */
    protected Button createDelButton(Grid table) {
        Button btn = new Button("-");
        btn.getElement()
                .setAttribute("style",
                        "font-size:20px;margin:0px;padding:0px;-webkit-border-radius:10px;-moz-border-radius:10px;-border-radius:10px;");
        return btn;
    }

    /**
     * Create a label with common format
     *
     * @param name
     * @return
     */
    protected Label createLabel(String name) {
        Label label = new Label(name);
        label.setWidth("100px");
        label.setHorizontalAlignment(HasHorizontalAlignment.ALIGN_CENTER);
        return label;
    }

    /**
     * Generate xml elements of configuration
     *
     * @param list List of properties
     * @param root xml element under which configuration tag is added
     * @param doc xml document
     */
    protected void configToXML(List<Property> list, Element root, Document doc) {

        Element confEle = null;

        for (Property prop : list) {
            if (prop.getName() != null && !prop.getName().matches("\\s*") && prop.getValue() != null
                    && !prop.getValue().matches("\\s*")) {

                if (confEle == null) {
                    confEle = doc.createElement("configuration");
                    root.appendChild(confEle);
                }

                // create <property>
                Element propEle = doc.createElement("property");
                confEle.appendChild(propEle);

                // create <name>
                Element nameEle = doc.createElement("name");
                propEle.appendChild(nameEle);
                nameEle.appendChild(doc.createTextNode(prop.getName()));

                // create <value>
                Element valEle = doc.createElement("value");
                propEle.appendChild(valEle);
                valEle.appendChild(doc.createTextNode(prop.getValue()));
            }
        }

    }

    /**
     * Generate xml elements of prepare tag
     *
     * @param list list of properties
     * @param root xml element under which prepare tag is added
     * @param doc xml document
     */
    protected void prepareToXML(List<Property> list, Element root, Document doc) {

        Element prepareEle = null;
        for (Property prop : list) {
            if (prop.getName() != null && !prop.getName().matches("\\s*") && prop.getValue() != null
                    && !prop.getValue().matches("\\s*")) {

                if (prepareEle == null) {
                    prepareEle = doc.createElement("prepare");
                    root.appendChild(prepareEle);
                }

                // create <delete> or <mkdir>
                Element ele = null;
                if (prop.getName().equals("delete")) {
                    ele = doc.createElement("delete");
                }
                else if (prop.getName().equals("mkdir")) {
                    ele = doc.createElement("mkdir");
                }
                ele.setAttribute("path", prop.getValue());
                prepareEle.appendChild(ele);
            }
        }
    }

    /**
     * Generate xml elements of specified tag name
     *
     * @param list list of properties
     * @param root xml element under which new elements are added
     * @param doc xml document
     * @param key tag name
     */
    protected void filterListToXML(List<Property> list, Element root, Document doc, String key) {

        for (Property prop : list) {
            if (prop.getName() != null && !prop.getName().matches("\\s*") && prop.getValue() != null
                    && !prop.getValue().matches("\\s*")) {
                if (prop.getName().equals(key)) {
                    // create key element
                    Element nameEle = doc.createElement(key);
                    root.appendChild(nameEle);

                    // create text node under created element
                    Text valEle = doc.createTextNode(prop.getValue());
                    nameEle.appendChild(valEle);
                }
            }
        }
    }

    /**
     * Generate xml element of specific tag using content of textbox
     *
     * @param doc xml document
     * @param tag tag name
     * @param box textbox
     * @return
     */
    protected Element generateElement(Document doc, String tag, TextBox box) {
        Element ele = doc.createElement(tag);
        Text t = doc.createTextNode(box.getText());
        ele.appendChild(t);
        return ele;
    }

    /**
     * Generate xml element of ok
     *
     * @param doc xml document
     * @param next next node widget to be executed after this in workflow
     * @return
     */
    protected Element generateOKElement(Document doc, NodeWidget next) {
        Element okEle = doc.createElement("ok");
        okEle.setAttribute("to", next.getName());
        return okEle;
    }

    /**
     * Generate xml element of error
     *
     * @param doc xml document
     * @return
     */
    protected Element generateErrorElement(Document doc) {
        Element errEle = doc.createElement("error");
        NodeWidget kill = getKillNode();
        errEle.setAttribute("to", kill == null ? "" : kill.getName());
        return errEle;
    }

    /**
     * Insert a row with textbox into a grid table
     *
     * @param grid grid table
     * @param row row number
     * @param label name of label
     * @return
     */
    protected TextBox insertTextRow(Grid grid, int row, String label) {
        grid.setWidget(row, 0, createLabel(label));
        TextBox box = new TextBox();
        grid.setWidget(row, 1, formatCell(box));
        return box;
    }

    /**
     * Insert a row for ok into a grid table
     *
     * @param grid grid table
     * @param row row number
     */
    protected void insertOKRow(Grid grid, int row) {
        grid.setWidget(row, 0, createLabel("OK"));
        okVal = new ListBox();
        updateWidgetDropDown();
        grid.setWidget(row, 1, formatCell(okVal));
    }

    /**
     * Insert a row for error into a grid table
     *
     * @param grid grid table
     * @param row row number
     */
    protected void insertErrorRow(Grid grid, int row) {
        grid.setWidget(row, 0, createLabel("Error"));
        errorVal = new ListBox();
        updateErrorDropDown();
        grid.setWidget(row, 1, formatCell(errorVal));
    }

    /**
     * Return a name of the node widget
     *
     * @return
     */
    public String getName() {
        String n = null;
        if (name != null) {
            n = name.getText();
        }
        else {
            n = new String("");
        }
        return n;
    }

    /**
     * Set a name of the node widget
     *
     * @param n
     */
    public void setName(String n) {
        if (name != null) {
            name.setText(n);
        }
    }

}
