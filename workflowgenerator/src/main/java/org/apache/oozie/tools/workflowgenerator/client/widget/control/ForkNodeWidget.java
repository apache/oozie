package org.apache.oozie.tools.workflowgenerator.client.widget.control;

import org.apache.oozie.tools.workflowgenerator.client.OozieWorkflowGenerator;
import org.apache.oozie.tools.workflowgenerator.client.property.control.ForkPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

/**
 * Class for node widget of fork node
 */
public class ForkNodeWidget extends NodeWidget {

    /**
     * Constructor which records oozie workflow generator and initializes
     * property table
     *
     * @param gen oozieWorkflowGenerator
     */
    public ForkNodeWidget(OozieWorkflowGenerator gen) {
        super(gen, "oozie-ForkNodeWidget");
    }

    /**
     * Update current lists of node widgets when clicked in workflow design
     * panel
     */
    @Override
    public void updateOnSelection() {
        table.updateWidgetDropDown();
        ((ForkPropertyTable) table).updateNeighborList();
        ((ForkPropertyTable) table).updateNeighborTable();
    }
}
