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

import java.util.ArrayList;
import java.util.List;

import org.apache.oozie.tools.workflowgenerator.client.property.Property;
import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of workflow
 */
public class WrkflowPropertyTable extends PropertyTable {

    private TextBox jt;
    private TextBox nn;
    private List<Property> configs;
    private ListBox namespace;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public WrkflowPropertyTable(NodeWidget w) {
        super(w);
        initConf();
        initWidget();
    }

    /**
     * Initialize configuration
     */
    protected void initConf() {
        configs = new ArrayList<Property>();
        configs.add(new Property("  ", "  "));
    }

    /**
     * Create a label with common format
     */
    @Override
    protected Label createLabel(String name) {
        Label label = new Label(name);
        label.setWidth("200px");
        label.setHeight("7px");
        label.setHorizontalAlignment(HasHorizontalAlignment.ALIGN_CENTER);
        return label;
    }

    /**
     * Generate xml elements of workflow (such as global section) and attach
     * them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        if (namespace != null && namespace.getSelectedIndex() < 3) {
            return;
        }

        // create <global>
        Element globalEle = doc.createElement("global");

        // create <job-tracker>
        if (jt.getText() != null && !jt.getText().matches("\\s*")) {
            globalEle.appendChild(generateElement(doc, "job-tracker", jt));
        }

        // create <name-node>
        if (nn.getText() != null && !nn.getText().matches("\\s*")) {
            globalEle.appendChild(generateElement(doc, "name-node", nn));
        }

        // create <configuration>
        configToXML(configs, globalEle, doc);

        // add <global> only when child nodes exist(if not, do not put <global>)
        if (globalEle.hasChildNodes()) {
            root.appendChild(globalEle);
        }
    }

    /**
     * Initialize widgets shown in propery table
     */
    protected void initWidget() {

        grid = new Grid(5, 2);
        this.add(grid);

        this.setAlwaysShowScrollBars(true);
        this.setSize("100%", "80%");

        // insert row for Name
        name = insertTextRow(grid, 0, "Name");
        name.setText("WrkflowGeneratorDemo"); // default value

        // insert row for Namespace
        grid.setWidget(1, 0, createLabel("NameSpace"));
        namespace = new ListBox();
        grid.setWidget(1, 1, createNameSpaceList(namespace));

        // insert row for JobTracker
        jt = insertTextRow(grid, 2, "Global JobTracker");

        // insert row for NameNode
        nn = insertTextRow(grid, 3, "Global NameNode");

        // insert row for Global Configuration
        grid.setWidget(4, 0, createLabel("Global Configuration"));
        grid.setWidget(4, 1, createSubTable("Property Name", "Value", configs, null));

    }

    /**
     * Create a drop down list of name spaces
     *
     * @param b list box
     * @return
     */
    protected ListBox createNameSpaceList(ListBox b) {
        if (b == null) {
            b = new ListBox();
        }
        b.addItem("uri:oozie:workflow:0.1");
        b.addItem("uri:oozie:workflow:0.2");
        b.addItem("uri:oozie:workflow:0.3");
        b.addItem("uri:oozie:workflow:0.4");
        b.setSelectedIndex(2);

        return b;
    }

    /**
     * Return a name space
     *
     * @return name space
     */
    public String getNameSpace() {
        return namespace.getItemText(namespace.getSelectedIndex());
    }

}
