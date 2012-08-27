package org.apache.oozie.tools.workflowgenerator.client.widget.control;

import org.apache.oozie.tools.workflowgenerator.client.OozieWorkflowGenerator;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

/**
 * Class for node widget of end node
 */
public class EndNodeWidget extends NodeWidget {

    /**
     * Constructor which records oozie workflow generator and initializes
     * property table
     *
     * @param gen oozieWorkflowGenerator
     */
    public EndNodeWidget(OozieWorkflowGenerator gen) {
        super(gen, "oozie-EndNodeWidget");
    }

    /**
     * Generate xml elements of end node and attach them to xml doc
     */
    @Override
    public void generateXML(Document doc, Element root, NodeWidget next) {
        Element endele = doc.createElement("end");
        endele.setAttribute("name", getName());
        root.appendChild(endele);
    }

    @Override
    protected void updateOnSelection() {
    }

}
