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

package org.apache.oozie.tools.workflowgenerator.client.property;

import org.apache.oozie.tools.workflowgenerator.client.property.action.EmailPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.action.FSPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.action.JavaPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.action.MapReducePropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.action.PigPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.action.PipesPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.action.SSHPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.action.ShellPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.action.StreamingPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.action.SubWFPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.control.DecisionPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.control.EndPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.control.ForkPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.control.JoinPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.control.KillPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.control.StartPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.EmailActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.FSActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.JavaActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.MapReduceActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.PigActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.PipesActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.SSHActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.ShellActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.StreamingActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.SubWFActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.DecisionNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.EndNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.ForkNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.JoinNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.KillNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.StartNodeWidget;

/**
 * Singleton class to instantiate property table corresponding to node widget.
 */
public class PropertyTableFactory {

    // Singleton
    private static PropertyTableFactory factory = new PropertyTableFactory();

    private PropertyTableFactory() {
    }

    /**
     * Return PropertyTableFactory instance
     *
     * @return PropertyTableFactory
     */
    public static PropertyTableFactory getInstance() {
        return factory;
    }

    /**
     * Return property table instance corresponding to node widget given in an
     * argument
     *
     * @param w node widget
     * @return PropertyTable
     */
    public PropertyTable createPropertyTable(NodeWidget w) {

        PropertyTable table = null;

        if (w instanceof MapReduceActionWidget) {
            table = new MapReducePropertyTable(w);
        }
        else if (w instanceof PigActionWidget) {
            table = new PigPropertyTable(w);
        }
        else if (w instanceof JavaActionWidget) {
            table = new JavaPropertyTable(w);
        }
        else if (w instanceof FSActionWidget) {
            table = new FSPropertyTable(w);
        }
        else if (w instanceof PipesActionWidget) {
            table = new PipesPropertyTable(w);
        }
        else if (w instanceof StreamingActionWidget) {
            table = new StreamingPropertyTable(w);
        }
        else if (w instanceof ShellActionWidget) {
            table = new ShellPropertyTable(w);
        }
        else if (w instanceof SSHActionWidget) {
            table = new SSHPropertyTable(w);
        }
        else if (w instanceof EmailActionWidget) {
            table = new EmailPropertyTable(w);
        }
        else if (w instanceof SubWFActionWidget) {
            table = new SubWFPropertyTable(w);
        }
        else if (w instanceof StartNodeWidget) {
            table = new StartPropertyTable(w);
        }
        else if (w instanceof EndNodeWidget) {
            table = new EndPropertyTable(w);
        }
        else if (w instanceof KillNodeWidget) {
            table = new KillPropertyTable(w);
        }
        else if (w instanceof ForkNodeWidget) {
            table = new ForkPropertyTable(w);
        }
        else if (w instanceof JoinNodeWidget) {
            table = new JoinPropertyTable(w);
        }
        else if (w instanceof DecisionNodeWidget) {
            table = new DecisionPropertyTable(w);
        }

        return table;
    }
}
