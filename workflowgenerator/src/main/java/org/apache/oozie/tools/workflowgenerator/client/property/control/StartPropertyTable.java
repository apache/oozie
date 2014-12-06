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

import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of start node
 */
public class StartPropertyTable extends PropertyTable {

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public StartPropertyTable(NodeWidget w) {
        super(w);
        initWidget();
    }

    /**
     * Generate xml elements of start node and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        // create <start>
        Element action = doc.createElement("start");
        action.setAttribute("to", okVal.getName());
    }

    /**
     * Initialize widgets shown in xml doc
     */
    protected void initWidget() {

        grid = new Grid(1, 2);
        this.add(grid);

        // Start widget doesn't expose Name TextBox to users, but internally
        // keep the data.
        name = new TextBox();
        name.setText("Start Node");

        this.setAlwaysShowScrollBars(true);
        this.setSize("100%", "80%");

        // insert row for OK
        insertOKRow(grid, 0);
    }
}
