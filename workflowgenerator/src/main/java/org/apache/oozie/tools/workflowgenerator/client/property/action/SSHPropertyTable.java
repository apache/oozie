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

package org.apache.oozie.tools.workflowgenerator.client.property.action;

import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

import com.google.gwt.cell.client.ButtonCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.cell.client.TextInputCell;
import com.google.gwt.xml.client.Text;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.RadioButton;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of SSH action
 */
public class SSHPropertyTable extends PropertyTable {

    private List<String> args;
    private TextBox host;
    private TextBox command;
    private RadioButton rby;
    private RadioButton rbn;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public SSHPropertyTable(NodeWidget w) {
        super(w);
        initConf();
        initWidget();
    }

    /**
     * Initialize configuration
     */
    protected void initConf() {
        args = new ArrayList<String>();
        args.add("");
    }

    /**
     * Generate xml elements of ssh action and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        Element action = doc.createElement("action");
        action.setAttribute("name", current.getName());

        // create <ssh>
        Element sshEle = doc.createElement("ssh");
        action.appendChild(sshEle);

        // create <host>
        sshEle.appendChild(generateElement(doc, "host", host));

        // create <command>
        sshEle.appendChild(generateElement(doc, "command", command));

        // create <args>
        for (String arg : args) {
            Element argsEle = doc.createElement("args");
            Text n = doc.createTextNode(arg);
            argsEle.appendChild(n);
            sshEle.appendChild(argsEle);

        }

        // create <capture-output>
        if (rby.getValue()) {
            Element outputele = doc.createElement("capture-output");
            sshEle.appendChild(outputele);
        }

        // create <ok>
        action.appendChild(generateOKElement(doc, next));

        // create <error>
        action.appendChild(generateErrorElement(doc));

        root.appendChild(action);
    }

    /**
     * initialize widgets shown in property table
     */
    protected void initWidget() {

        grid = new Grid(7, 2);
        this.add(grid);

        this.setAlwaysShowScrollBars(true);
        this.setSize("100%", "80%");

        // insert row for Name
        name = insertTextRow(grid, 0, "Name");

        // insert row for OK
        insertOKRow(grid, 1);

        // insert row for ERROR
        insertErrorRow(grid, 2);

        // insert row for Host
        host = insertTextRow(grid, 3, "Host");

        // insert row for Command
        command = insertTextRow(grid, 4, "Command");

        // insert row for Arguments
        grid.setWidget(5, 0, createLabel("Args"));
        grid.setWidget(5, 1, createArgsTable(args));

        // insert row for Capture Output
        grid.setWidget(6, 0, createLabel("Capture Output"));
        HorizontalPanel btnpanel = new HorizontalPanel();
        btnpanel.setVerticalAlignment(HasVerticalAlignment.ALIGN_BOTTOM);
        rbn = new RadioButton("outputGroup", "false");
        rby = new RadioButton("outputGroup", "true");
        rbn.setChecked(true);
        btnpanel.add(rby);
        btnpanel.add(rbn);
        grid.setWidget(6, 1, btnpanel);

    }

    /**
     * Create a table showing list of arguments added by a user
     *
     * @param data
     * @return
     */
    protected CellTable<String> createArgsTable(List<String> data) {
        final CellTable<String> table = new CellTable<String>();
        final ListDataProvider<String> dataProvider = new ListDataProvider<String>();
        dataProvider.setList(data);
        dataProvider.addDataDisplay(table);

        // Add Name column
        Column<String, String> argCol = null;

        // when editText is used for name column

        argCol = new Column<String, String>(new TextInputCell()) {
            @Override
            public String getValue(String object) {
                return object;
            }
        };

        // set event for updating value
        argCol.setFieldUpdater(new FieldUpdater<String, String>() {
            @Override
            public void update(int index, String object, String value) {
                List<String> li = dataProvider.getList();
                li.remove(index);
                li.add(index, value);
            }
        });
        table.addColumn(argCol, "");

        // Button to add row
        Column<String, String> addCol = new Column<String, String>(new ButtonCell()) {
            @Override
            public String getValue(String object) {
                return " + ";
            }
        };
        addCol.setFieldUpdater(new FieldUpdater<String, String>() {
            @Override
            public void update(int index, String object, String value) {
                List<String> li = dataProvider.getList();
                li.add(index + 1, new String(" "));
            }
        });

        table.addColumn(addCol, "");

        // Button to delete row
        Column<String, String> delCol = new Column<String, String>(new ButtonCell()) {
            @Override
            public String getValue(String object) {
                return " - ";
            }
        };

        delCol.setFieldUpdater(new FieldUpdater<String, String>() {
            @Override
            public void update(int index, String object, String value) {
                List<String> li = dataProvider.getList();
                li.remove(index);
                if (li.size() == 0) {
                    li.add(" ");
                    table.redraw();
                }
            }
        });

        table.addColumn(delCol, "");

        return table;

    }
}
