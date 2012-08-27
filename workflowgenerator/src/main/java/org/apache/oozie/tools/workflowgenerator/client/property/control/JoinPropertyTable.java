package org.apache.oozie.tools.workflowgenerator.client.property.control;

import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of join node
 */
public class JoinPropertyTable extends PropertyTable {

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public JoinPropertyTable(NodeWidget w) {
        super(w);
        initWidget();
    }

    /**
     * Generate xml elements of join node and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        Element action = doc.createElement("join");
        action.setAttribute("name", name.getText());
        action.setAttribute("to", next.getName());
        root.appendChild(action);

    }

    /**
     * Initialize widgets shown in property table
     */
    protected void initWidget() {

        grid = new Grid(2, 2);
        this.add(grid);

        this.setAlwaysShowScrollBars(true);
        this.setSize("100%", "80%");

        // insert row for name
        name = insertTextRow(grid, 0, "Name");

        // insert row for OK
        insertOKRow(grid, 1);
    }
}
