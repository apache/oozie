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

package org.apache.oozie.fluentjob.api.serialization;

import org.apache.oozie.fluentjob.api.action.Node;
import org.apache.oozie.fluentjob.api.generated.workflow.ObjectFactory;
import org.apache.oozie.fluentjob.api.generated.workflow.WORKFLOWAPP;
import org.apache.oozie.fluentjob.api.mapping.DozerBeanMapperSingleton;
import org.apache.oozie.fluentjob.api.dag.Graph;
import org.apache.oozie.fluentjob.api.workflow.Workflow;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Locale;

/**
 * Similar to JAXB {@link Marshaller} / {@link javax.xml.bind.Unmarshaller}, this class translates between Jobs API {@link Workflow}
 * and JAXB {@link WORKFLOWAPP} by using the appropriate Dozer converters.
 */
public class WorkflowMarshaller {

    public static String marshal(final Workflow workflow) throws JAXBException, UnsupportedEncodingException {
        final Graph graph = new Graph(workflow);
        final WORKFLOWAPP workflowapp = DozerBeanMapperSingleton.instance().map(graph, WORKFLOWAPP.class);
        final String filteredPackages = filterPackages(workflow);

        return marshal(workflowapp, filteredPackages);
    }

    private static String marshal(final WORKFLOWAPP workflowapp, final String filteredPackages)
            throws JAXBException, UnsupportedEncodingException {
        final JAXBElement<?> wfElement = new ObjectFactory().createWorkflowApp(workflowapp);

        final JAXBContext jc = JAXBContext.newInstance(filteredPackages);
        final Marshaller m =  jc.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        m.marshal(wfElement, out);

        return out.toString(Charset.defaultCharset().name());
    }

    private static String filterPackages(final Workflow workflow) {
        final StringBuilder filteredPackages = new StringBuilder();

        filteredPackages.append("org.apache.oozie.fluentjob.api.generated.workflow");
        appendIfPresent(workflow, filteredPackages, "distcp");
        appendIfPresent(workflow, filteredPackages, "email");
        appendIfPresent(workflow, filteredPackages, "git");
        appendIfPresent(workflow, filteredPackages, "hive2");
        appendIfPresent(workflow, filteredPackages, "hive");
        appendIfPresent(workflow, filteredPackages, "sla");
        appendIfPresent(workflow, filteredPackages, "shell");
        appendIfPresent(workflow, filteredPackages, "spark");
        appendIfPresent(workflow, filteredPackages, "sqoop");
        appendIfPresent(workflow, filteredPackages, "ssh");

        return filteredPackages.toString();
    }

    private static void appendIfPresent(final Workflow workflow, final StringBuilder filteredPackages, final String nodeType) {
        if (containsNodeType(workflow, nodeType)) {
            filteredPackages.append(":org.apache.oozie.fluentjob.api.generated.action.").append(nodeType);
        }
    }

    private static boolean containsNodeType(final Workflow workflow, final String nodeType) {
        final String actionType = nodeType + "action";
        for (final Node node : workflow.getAllNodes()) {
            final String nodeSimpleName = node.getClass().getSimpleName();
            if (nodeSimpleName.toLowerCase(Locale.getDefault()).startsWith(actionType.toLowerCase(Locale.getDefault()))) {
                return true;
            }
            if (node.getErrorHandler() != null) {
                final String errorHandlerSimpleName = node.getErrorHandler().getHandlerNode().getClass().getSimpleName();
                if (errorHandlerSimpleName.toLowerCase(Locale.getDefault())
                        .startsWith(actionType.toLowerCase(Locale.getDefault()))) {
                    return true;
                }
            }
        }

        return false;
    }
}
