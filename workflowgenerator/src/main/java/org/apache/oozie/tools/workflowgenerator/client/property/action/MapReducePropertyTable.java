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
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of MR action
 */
public class MapReducePropertyTable extends PropertyTable {

    protected List<Property> configs;
    protected List<Property> prepare;
    protected List<Property> others;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public MapReducePropertyTable(NodeWidget w) {
        super(w);
        init();
    }

    /**
     * Initialize a property table
     */
    protected void init() {
        initConf();
        initWidget();
    }

    /**
     * Initialize configuration
     */
    protected void initConf() {
        configs = new ArrayList<Property>();
        configs.add(new Property("mapred.mapper.class", ""));
        configs.add(new Property("mapred.reduce.class", ""));
        configs.add(new Property("mapred.job.queue.name", ""));

        prepare = new ArrayList<Property>();
        prepare.add(new Property("", ""));

        others = new ArrayList<Property>();
        others.add(new Property("", ""));
    }

    /**
     * Generate xml elements of mr action and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        Element action = doc.createElement("action");
        action.setAttribute("name", current.getName());

        // create <map-reduce>
        Element mrEle = doc.createElement("map-reduce");
        action.appendChild(mrEle);

        // create <job-tracker>
        mrEle.appendChild(generateElement(doc, "job-tracker", jt));

        // create <name-node>
        mrEle.appendChild(generateElement(doc, "name-node", nn));

        // create <prepare>
        prepareToXML(prepare, mrEle, doc);

        // create <job-xml>
        filterListToXML(others, mrEle, doc, "job-xml");

        // create <configuration>
        configToXML(configs, mrEle, doc);

        // create <file>
        filterListToXML(others, mrEle, doc, "file");

        // create <archive>
        filterListToXML(others, mrEle, doc, "archive");

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

        grid = new Grid(8, 2);
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

        // insert row for prepare
        grid.setWidget(5, 0, createLabel("Prepare"));
        grid.setWidget(5, 1, createSubTable("Operation", "Path", prepare, Arrays.asList("", "delete", "mkdir")));

        // insert row for Configuration
        grid.setWidget(6, 0, createLabel("Configuration"));
        grid.setWidget(6, 1, createSubTable("Property Name", "Value", configs, null));

        // insert row for others
        grid.setWidget(7, 0, createLabel("Others"));
        grid.setWidget(7, 1, createSubTable("Tag", "value", others, Arrays.asList("", "job-xml", "file", "archive")));
    }
}
