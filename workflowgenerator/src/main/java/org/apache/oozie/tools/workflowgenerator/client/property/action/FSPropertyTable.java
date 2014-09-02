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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

import com.google.gwt.cell.client.ButtonCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.cell.client.SelectionCell;
import com.google.gwt.cell.client.TextCell;
import com.google.gwt.cell.client.Cell.Context;
import com.google.gwt.cell.client.TextInputCell;
import com.google.gwt.safehtml.shared.SafeHtml;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of FS action
 */
public class FSPropertyTable extends PropertyTable {

    private List<FSActionData> fsdata;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w
     */
    public FSPropertyTable(NodeWidget w) {
        super(w);
        initConf();
        initWidget();
    }

    /**
     * Initialize configuration
     */
    private void initConf() {
        fsdata = new ArrayList<FSActionData>();
        fsdata.add(new FSActionData());
    }

    /**
     * Initialize widgets shown in property table
     */
    protected void initWidget() {

        VerticalPanel vertical = new VerticalPanel();
        this.add(vertical);
        grid = new Grid(3, 2);
        vertical.add(grid);

        this.setAlwaysShowScrollBars(true);
        this.setSize("100%", "80%");

        // insert row for Name
        name = insertTextRow(grid, 0, "Name");

        // insert row for OK
        insertOKRow(grid, 1);

        // insert row for ERROR
        insertErrorRow(grid, 2);

        // create cell table for FS operation
        vertical.add(createFSActionTable(fsdata));
    }

    /**
     * Create a table showing fs operations
     *
     * @param data
     * @return
     */
    protected CellTable<FSActionData> createFSActionTable(List<FSActionData> data) {

        final CellTable<FSActionData> table = new CellTable<FSActionData>();
        final ListDataProvider<FSActionData> dataProvider = new ListDataProvider<FSActionData>();
        dataProvider.setList(data);
        dataProvider.addDataDisplay(table);

        // Add Name column
        Column<FSActionData, String> nameCol = null;

        nameCol = new Column<FSActionData, String>(new SelectionCell(Arrays.asList("", "delete", "mkdir", "move",
                "chmod", "touchz"))) {
            @Override
            public String getValue(FSActionData object) {
                return object.getOp();
            }
        };

        // set event for updating value
        nameCol.setFieldUpdater(new FieldUpdater<FSActionData, String>() {
            @Override
            public void update(int index, FSActionData object, String value) {
                FSActionData d = dataProvider.getList().get(index);
                d.setOp(value);
                table.redraw();
            }
        });
        table.addColumn(nameCol, "operation");

        Column<FSActionData, String> label1Col = new Column<FSActionData, String>(new TextCell()) {
            @Override
            public String getValue(FSActionData object) {
                String rel = "Path";
                String op = object.getOp();
                if (op.equals("move")) {
                    rel = "Source Path";
                }
                return rel;
            }
        };

        table.addColumn(label1Col);

        // Add Column for 1st parameter of delete/mkdir/chmod/move/touchz
        Column<FSActionData, String> param1Col = new Column<FSActionData, String>(new TextInputCell()) {
            @Override
            public String getValue(FSActionData object) {
                String op = object.getOp();
                if (op.equals("delete") || op.equals("mkdir") || op.equals("chmod") || op.equals("touchz")) {
                    if (object.getParams().containsKey("path") && object.getParams().get("path") != null)
                        return object.getParams().get("path");
                }
                else if (op.equals("move")) {
                    if (object.getParams().containsKey("source") && object.getParams().get("source") != null)
                        return object.getParams().get("source");
                }
                return "";
            }
        };

        param1Col.setFieldUpdater(new FieldUpdater<FSActionData, String>() {
            @Override
            public void update(int index, FSActionData object, String value) {
                FSActionData d = dataProvider.getList().get(index);
                String op = d.getOp();
                if (op.equals("delete") || op.equals("mkdir") || op.equals("chmod") || op.equals("touchz")) {
                    d.getParams().put("path", value);
                }
                else if (op.equals("move")) {
                    d.getParams().put("source", value);
                }
            }
        });
        table.addColumn(param1Col, "");

        // Add Label for 2rd parameter of move and chmod
        Column<FSActionData, String> label2Col = new Column<FSActionData, String>(new TextCell()) {

            public void render(Context context, SafeHtml value, SafeHtmlBuilder sb) {
                if (value != null) {
                    FSActionData data = (FSActionData) context.getKey();
                    if (data.getOp().equals("move") || data.getOp().equals("chmod"))
                        sb.append(value);
                }
            }

            @Override
            public String getValue(FSActionData object) {
                String rel = null;
                String op = object.getOp();
                if (op.equals("move")) {
                    rel = "Target Path";
                }
                else if (op.equals("chmod")) {
                    rel = "Permissions";
                }
                return rel;
            }
        };

        table.addColumn(label2Col);

        // Add Column for 2nd parameter of move and chmod
        Column<FSActionData, String> param2Col = new Column<FSActionData, String>(new CustomEditTextCell()) {
            @Override
            public String getValue(FSActionData object) {
                String op = object.getOp();
                if (op.equals("move")) {
                    if (object.getParams().containsKey("target") && object.getParams().get("target") != null)
                        return object.getParams().get("target");
                }
                else if (op.equals("chmod")) {
                    if (object.getParams().containsKey("permissions") && object.getParams().get("permissions") != null)
                        return object.getParams().get("permissions");
                }
                return "";
            }
        };

        param2Col.setFieldUpdater(new FieldUpdater<FSActionData, String>() {
            @Override
            public void update(int index, FSActionData object, String value) {
                FSActionData d = dataProvider.getList().get(index);
                String op = d.getOp();
                if (op.equals("move")) {
                    d.getParams().put("target", value);
                }
                else if (op.equals("chmod")) {
                    d.getParams().put("permissions", value);
                }
            }
        });
        table.addColumn(param2Col, "");

        // Add Label for 3rd parameter of chmod
        Column<FSActionData, String> label3Col = new Column<FSActionData, String>(new TextCell()) {

            public void render(Context context, SafeHtml value, SafeHtmlBuilder sb) {
                if (value != null) {
                    FSActionData data = (FSActionData) context.getKey();
                    if (data.getOp().equals("chmod"))
                        sb.append(value);
                }
            }

            @Override
            public String getValue(FSActionData object) {
                String rel = null;
                String op = object.getOp();
                if (op.equals("chmod"))
                    rel = "Chmod files within directory?(dir-files)";
                return rel;
            }
        };

        table.addColumn(label3Col);

        // Add Column for 3rd parameter of chmod
        // ( Recursive option not implemented in this version. need to add
        // another column for that. )
        Column<FSActionData, String> param3Col = new Column<FSActionData, String>(new CustomSelectionCell(
                Arrays.asList("true", "false"))) {
            @Override
            public String getValue(FSActionData object) {
                String rel = null;
                String op = object.getOp();
                if (op.equals("chmod"))
                    rel = object.getParams().get("dir-files");
                return rel;
            }
        };

        param3Col.setFieldUpdater(new FieldUpdater<FSActionData, String>() {
            @Override
            public void update(int index, FSActionData object, String value) {
                FSActionData d = dataProvider.getList().get(index);
                String op = d.getOp();
                if (op.equals("chmod")) {
                    d.getParams().put("dir-files", value);
                }
            }
        });
        table.addColumn(param3Col, "");

        // Button to add row
        Column<FSActionData, String> addCol = new Column<FSActionData, String>(new ButtonCell()) {
            @Override
            public String getValue(FSActionData object) {
                return " + ";
            }
        };
        addCol.setFieldUpdater(new FieldUpdater<FSActionData, String>() {
            @Override
            public void update(int index, FSActionData object, String value) {
                dataProvider.getList().add(index + 1, new FSActionData());
            }
        });

        table.addColumn(addCol, "");

        // Button to delete row
        Column<FSActionData, String> delCol = new Column<FSActionData, String>(new ButtonCell()) {
            @Override
            public String getValue(FSActionData object) {
                return " - ";
            }
        };

        delCol.setFieldUpdater(new FieldUpdater<FSActionData, String>() {

            @Override
            public void update(int index, FSActionData object, String value) {
                List<FSActionData> li = dataProvider.getList();
                if (li.size() == 1) {
                    FSActionData p = li.get(0);
                    p.clear();
                    table.redraw();
                }
                else {
                    dataProvider.getList().remove(index);
                }
            }
        });

        table.addColumn(delCol, "");

        return table;
    }

    class CustomEditTextCell extends TextInputCell {

        @Override
        public void render(Context context, String value, SafeHtmlBuilder sb) {
            FSActionData fsdata = (FSActionData) context.getKey();
            String op = fsdata.getOp();
            if (op.equals("move") || op.equals("chmod"))
                super.render(context, value, sb);

        }
    }

    class CustomSelectionCell extends SelectionCell {

        public CustomSelectionCell(List<String> options) {
            super(options);
        }

        @Override
        public void render(Context context, String value, SafeHtmlBuilder sb) {
            FSActionData fsdata = (FSActionData) context.getKey();
            String op = fsdata.getOp();
            if (op.equals("chmod")) {
                super.render(context, value, sb);
            }
        }
    }

    /**
     * Generate xml elements of fs action and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        Element action = doc.createElement("action");
        action.setAttribute("name", current.getName());

        Element fsEle = doc.createElement("fs");
        action.appendChild(fsEle);

        // create <delete>
        fsDataListToXML(fsdata, fsEle, doc, "delete");

        // create <mkdir>
        fsDataListToXML(fsdata, fsEle, doc, "mkdir");

        // create <move>
        fsDataListToXML(fsdata, fsEle, doc, "move");

        // create <chmod>
        fsDataListToXML(fsdata, fsEle, doc, "chmod");

        // create <touchz>
        fsDataListToXML(fsdata, fsEle, doc, "touchz");

        // create <ok>
        action.appendChild(generateOKElement(doc, next));

        // create <error>
        action.appendChild(generateErrorElement(doc));

        root.appendChild(action);

    }

    /**
     * Generate xml elements of fs operations
     *
     * @param list list of FS operations
     * @param root xml element under which fs action is added
     * @param doc xml document
     * @param key name of fs operation
     */
    protected void fsDataListToXML(List<FSActionData> list, Element root, Document doc, String key) {

        for (FSActionData fsdata : list) {
            String op = fsdata.getOp();

            if (op != null && !op.matches("\\s*") && op.equals(key)) {

                Map<String, String> params = fsdata.getParams();

                boolean flag = true;
                if (op.equals("delete") && !params.containsKey("path")) {
                    flag = false;
                }
                else if (op.equals("mkdir") && !params.containsKey("path")) {
                    flag = false;
                }
                else if (op.equals("move") && (!params.containsKey("source") || !params.containsKey("target"))) {
                    flag = false;
                }
                else if (op.equals("chmod") && (!params.containsKey("path") || !params.containsKey("permissions"))) {
                    flag = false;
                }
                else if (op.equals("touchz") && (!params.containsKey("path"))) {
                    flag = false;
                }

                if (flag) {
                    // create key element
                    Element nameele = doc.createElement(key);
                    root.appendChild(nameele);

                    // set attribute for parameter(s)
                    for (Map.Entry<String, String> e : params.entrySet()) {
                        nameele.setAttribute(e.getKey(), e.getValue());
                    }
                }
            }
        }

    }

    /**
     * class to store FS operation and relevant parameters
     */
    public class FSActionData {

        private String op;
        private Map<String, String> params;

        public FSActionData() {
            params = new HashMap<String, String>();
            op = "";
        }

        public String getOp() {
            return op;
        }

        public void setOp(String s) {
            this.op = s;
        }

        public Map<String, String> getParams() {
            return params;
        }

        public void clear() {
            op = "";
            params.clear();
        }
    }

}
