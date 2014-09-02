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
import java.util.List;

import org.apache.oozie.tools.workflowgenerator.client.property.Property;
import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.RadioButton;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of shell action
 */
public class ShellPropertyTable extends PropertyTable {

    private TextBox jt;
    private TextBox nn;
    private TextBox exec;
    private List<Property> configs;
    private List<Property> prepare;
    private List<Property> others;
    private RadioButton rby;
    private RadioButton rbn;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w
     */
    public ShellPropertyTable(NodeWidget w) {
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

        prepare = new ArrayList<Property>();
        prepare.add(new Property("", ""));

        others = new ArrayList<Property>();
        others.add(new Property("", ""));
    }

    /**
     * Generate xml elements of shell action and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        Element action = doc.createElement("action");
        action.setAttribute("name", current.getName());

        // create <shell>
        Element shellEle = doc.createElement("shell");
        action.appendChild(shellEle);

        // create <job-tracker>
        shellEle.appendChild(generateElement(doc, "job-tracker", jt));

        // create <name-node>
        shellEle.appendChild(generateElement(doc, "name-node", nn));

        // create <prepare>
        prepareToXML(prepare, shellEle, doc);

        // create <job-xml>
        filterListToXML(others, shellEle, doc, "job-xml");

        // create <configuration>
        configToXML(configs, shellEle, doc);

        // create <exec>
        shellEle.appendChild(generateElement(doc, "exec", exec));

        // create <argument>
        filterListToXML(others, shellEle, doc, "argument");

        // create <env-var>
        filterListToXML(others, shellEle, doc, "env-var");

        // create <file>
        filterListToXML(others, shellEle, doc, "file");

        // create <archive>
        filterListToXML(others, shellEle, doc, "archive");

        // create <capture-output>
        if (rby.getValue()) {
            Element outputele = doc.createElement("capture-output");
            shellEle.appendChild(outputele);
        }

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

        grid = new Grid(10, 2);
        this.add(grid);

        this.setAlwaysShowScrollBars(true);
        this.setSize("100%", "80%");

        // insert row for Name
        name = insertTextRow(grid, 0, "Name");

        // insert row for OK
        insertOKRow(grid, 1);

        // insert row for ERROR
        insertErrorRow(grid, 2);

        // insert row for JobTracker
        jt = insertTextRow(grid, 3, "JobTracker");

        // insert row for NameNode
        nn = insertTextRow(grid, 4, "NameNode");

        // insert row for Exec
        exec = insertTextRow(grid, 5, "Exec");

        // insert row for Parameter
        grid.setWidget(6, 0, createLabel("Parameter"));
        grid.setWidget(
                6,
                1,
                createSubTable("Tag", "value", others,
                        Arrays.asList("", "argument", "job-xml", "env-var", "file", "archive")));

        // insert row for Configuration
        grid.setWidget(7, 0, createLabel("Configuration"));
        grid.setWidget(7, 1, createSubTable("Property Name", "Value", configs, null));

        // insert row for prepare
        grid.setWidget(8, 0, createLabel("Prepare"));
        grid.setWidget(8, 1, createSubTable("Operation", "Path", prepare, Arrays.asList("", "delete", "mkdir")));

        // insert row for Capture output
        grid.setWidget(9, 0, createLabel("Capture Output"));
        HorizontalPanel btnpanel = new HorizontalPanel();
        btnpanel.setVerticalAlignment(HasVerticalAlignment.ALIGN_BOTTOM);
        rbn = new RadioButton("outputGroup", "false");
        rby = new RadioButton("outputGroup", "true");
        rbn.setChecked(true);
        btnpanel.add(rby);
        btnpanel.add(rbn);
        grid.setWidget(9, 1, btnpanel);

    }
}
