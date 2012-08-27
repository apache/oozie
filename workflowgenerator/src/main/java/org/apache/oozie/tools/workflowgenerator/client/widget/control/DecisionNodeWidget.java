package org.apache.oozie.tools.workflowgenerator.client.widget.control;

import org.apache.oozie.tools.workflowgenerator.client.OozieWorkflowGenerator;
import org.apache.oozie.tools.workflowgenerator.client.property.control.DecisionPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

/**
 * Class for node widget of decision node
 */
public class DecisionNodeWidget extends NodeWidget {

    /**
     * Constructor which records oozie workflow generator and initializes
     * property table
     *
     * @param gen oozieWorkflowGenerator
     */
    public DecisionNodeWidget(OozieWorkflowGenerator gen) {
        super(gen, "oozie-DecisionNodeWidget");
    }

    /**
     * Update a list of node widgets that this node widget has connection to
     */
    @Override
    protected void updateOnSelection() {
        table.updateWidgetDropDown();
        ((DecisionPropertyTable) table).updateNeighborList();
        ((DecisionPropertyTable) table).updateNeighborTable();
    }
}
