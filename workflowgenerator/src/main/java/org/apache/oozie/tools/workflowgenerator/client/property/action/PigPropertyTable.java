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
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of pig action
 */
public class PigPropertyTable extends PropertyTable {

    private List<Property> configs;
    private List<Property> prepare;
    private List<Property> others;
    private TextBox script;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public PigPropertyTable(NodeWidget w) {

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

        grid = new Grid(9, 2);
        this.add(grid);

        this.setAlwaysShowScrollBars(true);
        this.setSize("100%", "80%");

        // insert row for Name
        name = insertTextRow(grid, 0, "Name");

        // insert row for OK
        insertOKRow(grid, 1);

        // insert row for ERROR
        insertErrorRow(grid, 2);

        // insert row for Script
        script = insertTextRow(grid, 3, "Script");

        // insert row for JobTracker
        jt = insertTextRow(grid, 4, "JobTracker");

        // insert row for NameNode
        nn = insertTextRow(grid, 5, "NameNode");

        // insert row for Configuration
        grid.setWidget(6, 0, createLabel("Configuration"));
        grid.setWidget(6, 1, createSubTable("Property Name", "Value", configs, null));

        // insert row for prepare
        grid.setWidget(7, 0, createLabel("Prepare"));
        grid.setWidget(7, 1, createSubTable("Operation", "Path", prepare, Arrays.asList("", "delete", "mkdir")));

        // insert row for others
        grid.setWidget(8, 0, createLabel("Others"));
        grid.setWidget(
                8,
                1,
                createSubTable("Tag", "value", others,
                        Arrays.asList("", "job-xml", "argument", "param", "file", "archive")));

    }

    /**
     * Generate xml elements of pig action and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        Element action = doc.createElement("action");
        action.setAttribute("name", current.getName());

        // create <pig>
        Element pigEle = doc.createElement("pig");
        action.appendChild(pigEle);

        // create <job-tracker>
        pigEle.appendChild(generateElement(doc, "job-tracker", jt));

        // create <name-node>
        pigEle.appendChild(generateElement(doc, "name-node", nn));

        // create <prepare>
        prepareToXML(prepare, pigEle, doc);

        // create <job-xml>
        filterListToXML(others, pigEle, doc, "job-xml");

        // create <configuration>
        configToXML(configs, pigEle, doc);

        // create <script>
        pigEle.appendChild(generateElement(doc, "script", script));

        // create <param>
        filterListToXML(others, pigEle, doc, "param");

        // create <argument>
        filterListToXML(others, pigEle, doc, "argument");

        // create <file>
        filterListToXML(others, pigEle, doc, "file");

        // create <archive>
        filterListToXML(others, pigEle, doc, "archive");

        // create <ok>
        action.appendChild(generateOKElement(doc, next));

        // create <error>
        action.appendChild(generateErrorElement(doc));

        root.appendChild(action);

    }
}
