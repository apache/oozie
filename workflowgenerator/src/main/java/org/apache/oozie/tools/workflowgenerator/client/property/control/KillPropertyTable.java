package org.apache.oozie.tools.workflowgenerator.client.property.control;

import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

import com.google.gwt.user.client.ui.Grid;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for property table of kill node
 */
public class KillPropertyTable extends PropertyTable {

    private TextBox message;

    /**
     * Constructor which records node widget and initializes
     *
     * @param w node widget
     */
    public KillPropertyTable(NodeWidget w) {
        super(w);
        initWidget();
    }

    /**
     * Generate xml elements of kill node and attach them to xml doc
     */
    public void generateXML(Document doc, Element root, NodeWidget next) {

        // create <kill>
        Element action = doc.createElement("kill");
        root.appendChild(action);
        action.setAttribute("name", name.getText());

        // create <message>
        action.appendChild(generateElement(doc, "message", message));
    }

    /**
     * Initialize widgets shown in property table
     */
    protected void initWidget() {

        grid = new Grid(2, 2);
        this.add(grid);

        this.setAlwaysShowScrollBars(true);
        this.setSize("100%", "80%");

        // insert row for Name
        name = insertTextRow(grid, 0, "Name");

        // insert row for Message
        message = insertTextRow(grid, 1, "Message");
    }
}
