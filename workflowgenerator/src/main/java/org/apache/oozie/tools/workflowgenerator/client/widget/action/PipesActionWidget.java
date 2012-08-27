package org.apache.oozie.tools.workflowgenerator.client.widget.action;

import org.apache.oozie.tools.workflowgenerator.client.OozieWorkflowGenerator;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

/**
 * Class for node widget of MR pipes action
 */
public class PipesActionWidget extends NodeWidget {

    /**
     * Constructor which records oozie workflow generator and initializes
     * property table
     *
     * @param gen oozieWorkflowGenerator
     */
    public PipesActionWidget(OozieWorkflowGenerator gen) {
        super(gen, "oozie-PipesActionWidget");
    }

    /**
     * Update current lists of created node widgets when clicked in workflow
     * design panel
     */
    @Override
    public void updateOnSelection() {
        table.updateWidgetDropDown();
        table.updateErrorDropDown();
    }
}
