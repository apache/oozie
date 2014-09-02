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

import org.apache.oozie.tools.workflowgenerator.client.property.Property;
import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

import com.google.gwt.xml.client.Text;
import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.RadioButton;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of subworkflow action
 */
public class SubWFPropertyTable extends PropertyTable {

    private List<Property> configs;
    private TextBox apppath;
    private RadioButton rby;
    private RadioButton rbn;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public SubWFPropertyTable(NodeWidget w) {
        super(w);
        initConf();
        initWidget();
    }

    /**
     * Initialize configuration
     */
    protected void initConf() {
        configs = new ArrayList<Property>();
        configs.add(new Property("mapred.job.queue.name", ""));
    }

    /**
     * Generate xml elements of subworkflow action and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        Element action = doc.createElement("action");
        action.setAttribute("name", current.getName());

        // create <sub-workflow>
        Element subwfEle = doc.createElement("sub-workflow");
        action.appendChild(subwfEle);

        // create <app-path>
        subwfEle.appendChild(generateElement(doc, "app-path", apppath));

        // create <propagate-configuration>
        if (rby.getValue()) {
            Element proele = doc.createElement("propagate-configuration");
            subwfEle.appendChild(proele);
        }

        // create <configuration>
        configToXML(configs, subwfEle, doc);

        // create <ok>
        action.appendChild(generateOKElement(doc, next));

        // create <error>
        action.appendChild(generateErrorElement(doc));

        root.appendChild(action);
    }

    /**
     * Initialize widgets shown in property table
     */
    protected void initWidget() {

        grid = new Grid(6, 2);
        this.add(grid);

        this.setAlwaysShowScrollBars(true);
        this.setSize("100%", "80%");

        // insert row for Name
        name = insertTextRow(grid, 0, "Name");

        // insert row for OK
        insertOKRow(grid, 1);

        // insert row for ERROR
        insertErrorRow(grid, 2);

        // insert row for App path
        apppath = insertTextRow(grid, 3, "App Path");

        // insert row for Propagate config
        grid.setWidget(4, 0, createLabel("Propagate Config"));
        HorizontalPanel radiopanel = new HorizontalPanel();
        radiopanel.setVerticalAlignment(HasVerticalAlignment.ALIGN_BOTTOM);
        rbn = new RadioButton("propagateGroup", "false");
        rby = new RadioButton("propagateGroup", "true");
        rbn.setChecked(true);
        radiopanel.add(rby);
        radiopanel.add(rbn);
        grid.setWidget(4, 1, radiopanel);

        // insert row for Configuration
        grid.setWidget(5, 0, createLabel("Configuration"));
        grid.setWidget(5, 1, createSubTable("Property Name", "Value", configs, null));

    }

}
