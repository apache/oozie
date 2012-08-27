package org.apache.oozie.tools.workflowgenerator.client.widget.control;

import org.apache.oozie.tools.workflowgenerator.client.OozieWorkflowGenerator;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;

/**
 * Class for node widget of kill node
 */
public class KillNodeWidget extends NodeWidget {

    /**
     * Constructor which records oozie workflow generator and initializes
     * property table
     *
     * @param gen oozieWorkflowGenerator
     */
    public KillNodeWidget(OozieWorkflowGenerator gen) {
        super(gen, "oozie-KillNodeWidget");
    }

    @Override
    protected void updateOnSelection() {
    }
}
