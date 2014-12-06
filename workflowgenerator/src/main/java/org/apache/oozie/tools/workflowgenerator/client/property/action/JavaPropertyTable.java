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
 * Class for property table of java action
 */
public class JavaPropertyTable extends PropertyTable {

    private List<Property> configs;
    private List<Property> prepare;
    private List<Property> others;
    private RadioButton rby;
    private RadioButton rbn;
    private TextBox mainClass;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public JavaPropertyTable(NodeWidget w) {

        super(w);
        initConf();
        initWidget();
    }

    /**
     * Initialize configuration
     */
    private void initConf() {
        configs = new ArrayList<Property>();
        configs.add(new Property("mapred.job.queue.name", ""));

        prepare = new ArrayList<Property>();
        prepare.add(new Property("", ""));

        others = new ArrayList<Property>();
        others.add(new Property("", ""));
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

        // insert row for Configuration
        grid.setWidget(5, 0, createLabel("Configuration"));
        grid.setWidget(5, 1, createSubTable("Property Name", "Value", configs, null));

        // insert row for Main Class
        mainClass = insertTextRow(grid, 6, "Main Class");

        // insert row for prepare
        grid.setWidget(7, 0, createLabel("Prepare"));
        grid.setWidget(7, 1, createSubTable("Operation", "Path", prepare, Arrays.asList("", "delete", "mkdir")));

        // insert row for others
        grid.setWidget(8, 0, createLabel("Others"));
        grid.setWidget(
                8,
                1,
                createSubTable("Tag", "value", others,
                        Arrays.asList("", "job-xml", "java-opts", "arg", "file", "archive")));

        // insert row for Capture Output
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

    /**
     * Generate xml elements of java action and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        Element action = doc.createElement("action");
        action.setAttribute("name", current.getName());

        // create <java>
        Element javaEle = doc.createElement("java");
        action.appendChild(javaEle);

        // create <job-tracker>
        javaEle.appendChild(generateElement(doc, "job-tracker", jt));

        // create <name-node>
        javaEle.appendChild(generateElement(doc, "name-node", nn));

        // create <prepare>
        prepareToXML(prepare, javaEle, doc);

        // create <job-xml>
        filterListToXML(others, javaEle, doc, "job-xml");

        // create <configuration>
        configToXML(configs, javaEle, doc);

        // create <main-class>
        javaEle.appendChild(generateElement(doc, "main-class", mainClass));

        // create <java-opts>
        filterListToXML(others, javaEle, doc, "java-opts");

        // create <arg>
        filterListToXML(others, javaEle, doc, "arg");

        // create <file>
        filterListToXML(others, javaEle, doc, "file");

        // create <archive>
        filterListToXML(others, javaEle, doc, "archive");

        // create <capture-output>
        if (rby.getValue()) {
            Element outputele = doc.createElement("capture-output");
            javaEle.appendChild(outputele);
        }

        // create <ok>
        action.appendChild(generateOKElement(doc, next));

        // create <error>
        action.appendChild(generateErrorElement(doc));

        root.appendChild(action);
    }
}
