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
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

import com.google.gwt.xml.client.Text;
import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of MR pipes action
 */
public class PipesPropertyTable extends MapReducePropertyTable {

    private List<Property> pipes;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public PipesPropertyTable(NodeWidget w) {
        super(w);
    }

    /**
     * Initialize a property table
     */
    @Override
    protected void init() {
        initConf();
        initWidget();
    }

    /**
     * Initialize configuration
     */
    @Override
    protected void initConf() {
        configs = new ArrayList<Property>();
        configs.add(new Property("mapred.job.queue.name", ""));

        prepare = new ArrayList<Property>();
        prepare.add(new Property("", ""));

        others = new ArrayList<Property>();
        others.add(new Property("", ""));

        pipes = new ArrayList<Property>();
        pipes.add(new Property("", ""));
    }

    /**
     * Generate xml elements of pipes action and attach them to xml doc
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

        // create <pipes>
        Element pipesEle = null;
        pipesEle = filterListToXML(pipes, mrEle, pipesEle, doc, "map");
        pipesEle = filterListToXML(pipes, mrEle, pipesEle, doc, "reduce");
        pipesEle = filterListToXML(pipes, mrEle, pipesEle, doc, "inputformat");
        pipesEle = filterListToXML(pipes, mrEle, pipesEle, doc, "partitioner");
        pipesEle = filterListToXML(pipes, mrEle, pipesEle, doc, "writer");
        pipesEle = filterListToXML(pipes, mrEle, pipesEle, doc, "program");

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
     * Create xml element of specified tag name
     *
     * @param list list of properties
     * @param root xml element under which new elements are added
     * @param pipes pipes element
     * @param doc xml doc
     * @param key tag name
     * @return
     */
    protected Element filterListToXML(List<Property> list, Element root, Element pipes, Document doc, String key) {

        for (Property prop : list) {
            if (prop.getName() != null && !prop.getName().matches("\\s*") && prop.getValue() != null
                    && !prop.getValue().matches("\\s*")) {
                if (prop.getName().equals(key)) {
                    if (pipes == null) {
                        pipes = doc.createElement("pipes");
                        root.appendChild(pipes);
                    }
                    // create key element
                    Element nameele = doc.createElement(key);
                    pipes.appendChild(nameele);

                    // create text node under created element
                    Text valele = doc.createTextNode(prop.getValue());
                    nameele.appendChild(valele);
                }
            }
        }
        return pipes;
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

        // insert row for Pipes setting
        grid.setWidget(3, 0, createLabel("Pipes Setting"));
        grid.setWidget(
                3,
                1,
                createSubTable("Tag", "value", pipes,
                        Arrays.asList("", "map", "reduce", "inputformat", "partitioner", "writer", "program")));

        // insert row for JobTracker
        jt = insertTextRow(grid, 4, "JobTracker");

        // insert row for NameNode
        nn = insertTextRow(grid, 5, "NameNode");

        // insert row for prepare
        grid.setWidget(6, 0, createLabel("Prepare"));
        grid.setWidget(6, 1, createSubTable("Operation", "Path", prepare, Arrays.asList("", "delete", "mkdir")));

        // insert row for Configuration
        grid.setWidget(7, 0, createLabel("Configuration"));
        grid.setWidget(7, 1, createSubTable("Property Name", "Value", configs, null));

        // insert row for others
        grid.setWidget(8, 0, createLabel("Others"));
        grid.setWidget(8, 1, createSubTable("Tag", "value", others, Arrays.asList("", "job-xml", "file", "archive")));

    }
}
