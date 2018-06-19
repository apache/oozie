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

package org.apache.oozie.jobs.client.jaxb;

import org.apache.oozie.fluentjob.api.generated.workflow.ACTION;
import org.apache.oozie.fluentjob.api.generated.workflow.ACTIONTRANSITION;
import org.apache.oozie.fluentjob.api.generated.workflow.CONFIGURATION;
import org.apache.oozie.fluentjob.api.generated.workflow.DELETE;
import org.apache.oozie.fluentjob.api.generated.workflow.END;
import org.apache.oozie.fluentjob.api.generated.workflow.KILL;
import org.apache.oozie.fluentjob.api.generated.workflow.MAPREDUCE;
import org.apache.oozie.fluentjob.api.generated.workflow.ObjectFactory;
import org.apache.oozie.fluentjob.api.generated.workflow.PREPARE;
import org.apache.oozie.fluentjob.api.generated.workflow.START;
import org.apache.oozie.fluentjob.api.generated.workflow.WORKFLOWAPP;

import org.junit.Test;
import org.xml.sax.SAXException;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.Comparison;
import org.xmlunit.diff.ComparisonResult;
import org.xmlunit.diff.ComparisonType;
import org.xmlunit.diff.Diff;
import org.xmlunit.diff.DifferenceEvaluator;
import org.xmlunit.diff.DifferenceEvaluators;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * This class tests whether the workflow.xml files are parsed correctly into JAXB objects and whether the JAXB objects
 * are serialized correctly to xml.
 */
public class TestJAXBWorkflow {
    private static final String GENERATED_PACKAGES_ALL = "org.apache.oozie.fluentjob.api.generated.action.distcp:" +
            "org.apache.oozie.fluentjob.api.generated.action.email:" +
            "org.apache.oozie.fluentjob.api.generated.action.hive2:" +
            "org.apache.oozie.fluentjob.api.generated.action.hive:" +
            "org.apache.oozie.fluentjob.api.generated.sla:" +
            "org.apache.oozie.fluentjob.api.generated.workflow:" +
            "org.apache.oozie.fluentjob.api.generated.action.shell:" +
            "org.apache.oozie.fluentjob.api.generated.action.spark:" +
            "org.apache.oozie.fluentjob.api.generated.action.sqoop:" +
            "org.apache.oozie.fluentjob.api.generated.action.ssh";
    private static final String GENERATED_PACKAGES_WORKFLOW = "org.apache.oozie.fluentjob.api.generated.workflow";
    private static final String WORKFLOW_MAPREDUCE_ACTION = "/workflow-mapreduce-action.xml";
    private static final String WORKFLOW_ALL_ACTIONS = "/workflow-all-actions.xml";

    /**
     * Tests whether a workflow.xml object is parsed correctly into a JAXB element tree by checking some of the main
     * properties.
     * @throws SAXException If a SAX error occurs during parsing the schema file.
     * @throws JAXBException If an error was encountered while creating the <tt>JAXBContext</tt> or the
     *         <tt>Unmarshaller</tt> objects.
     */
    @Test
    public void whenWorkflowXmlWithAllActionTypesIsUnmarshalledAttributesArePreserved()
            throws SAXException, JAXBException, URISyntaxException {
        final WORKFLOWAPP wf = unmarshalWorkflowWithAllActionTypes();

        assertEquals("jaxb-example-wf", wf.getName());
        assertEquals("mr-node", wf.getStart().getTo());
        assertEquals("end", wf.getEnd().getName());

        final List<Object> actions = wf.getDecisionOrForkOrJoin();

        final KILL kill = (KILL) actions.get(9);
        assertKill(kill);

        final MAPREDUCE mr = ((ACTION) actions.get(0)).getMapReduce();
        assertMapReduce(mr);

        final org.apache.oozie.fluentjob.api.generated.action.distcp.ACTION distcp =
                (org.apache.oozie.fluentjob.api.generated.action.distcp.ACTION)
                        ((JAXBElement<?>) ((ACTION) actions.get(1)).getOther()).getValue();
        assertDistcp(distcp);

        final org.apache.oozie.fluentjob.api.generated.action.email.ACTION email =
                (org.apache.oozie.fluentjob.api.generated.action.email.ACTION)
                        ((JAXBElement<?>) ((ACTION) actions.get(2)).getOther()).getValue();
        assertEmail(email);

        final org.apache.oozie.fluentjob.api.generated.action.hive2.ACTION hive2 =
                (org.apache.oozie.fluentjob.api.generated.action.hive2.ACTION)
                        ((JAXBElement<?>) ((ACTION) actions.get(3)).getOther()).getValue();
        assertHive2(hive2);

        final org.apache.oozie.fluentjob.api.generated.action.hive.ACTION hive =
                (org.apache.oozie.fluentjob.api.generated.action.hive.ACTION)
                        ((JAXBElement<?>) ((ACTION) actions.get(4)).getOther()).getValue();
        assertHive(hive);

        final org.apache.oozie.fluentjob.api.generated.action.shell.ACTION shell =
                (org.apache.oozie.fluentjob.api.generated.action.shell.ACTION)
                        ((JAXBElement<?>) ((ACTION) actions.get(5)).getOther()).getValue();
        assertShell(shell);

        final org.apache.oozie.fluentjob.api.generated.action.spark.ACTION spark =
                (org.apache.oozie.fluentjob.api.generated.action.spark.ACTION)
                        ((JAXBElement<?>) ((ACTION) actions.get(6)).getOther()).getValue();
        assertSpark(spark);

        final org.apache.oozie.fluentjob.api.generated.action.sqoop.ACTION sqoop =
                (org.apache.oozie.fluentjob.api.generated.action.sqoop.ACTION)
                        ((JAXBElement<?>) ((ACTION) actions.get(7)).getOther()).getValue();
        assertSqoop(sqoop);

        final org.apache.oozie.fluentjob.api.generated.action.ssh.ACTION ssh =
                (org.apache.oozie.fluentjob.api.generated.action.ssh.ACTION)
                        ((JAXBElement<?>) ((ACTION) actions.get(8)).getOther()).getValue();
        assertSsh(ssh);
    }

    private void assertKill(final KILL kill) {
        assertEquals("fail", kill.getName());
        assertEquals("Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]", kill.getMessage());
    }

    private void assertMapReduce(final MAPREDUCE mr) {
        final PREPARE prepare = mr.getPrepare();
        assertEquals(0, prepare.getMkdir().size());

        final List<DELETE> deleteList = prepare.getDelete();
        assertEquals(1, deleteList.size());

        final DELETE delete = deleteList.get(0);
        assertEquals("${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/${outputDir}", delete.getPath());

        final CONFIGURATION conf = mr.getConfiguration();
        final List<CONFIGURATION.Property> properties = conf.getProperty();

        final CONFIGURATION.Property mapper = properties.get(1);
        assertEquals("mapred.mapper.class", mapper.getName());
        assertEquals("org.apache.oozie.example.SampleMapper", mapper.getValue());
    }

    private void assertDistcp(final org.apache.oozie.fluentjob.api.generated.action.distcp.ACTION distcp) {
        assertEquals(1, distcp.getPrepare().getDelete().size());
        assertEquals(1, distcp.getPrepare().getMkdir().size());
        assertEquals(2, distcp.getConfiguration().getProperty().size());
        assertEquals(2, distcp.getArg().size());
    }

    private void assertEmail(final org.apache.oozie.fluentjob.api.generated.action.email.ACTION email) {
        assertEquals("foo@bar.com", email.getTo());
        assertEquals("foo", email.getSubject());
        assertEquals("bar", email.getBody());
    }

    private void assertHive2(final org.apache.oozie.fluentjob.api.generated.action.hive2.ACTION hive2) {
        assertEquals(1, hive2.getPrepare().getDelete().size());
        assertEquals(1, hive2.getConfiguration().getProperty().size());
        assertEquals(2, hive2.getParam().size());
    }

    private void assertHive(final org.apache.oozie.fluentjob.api.generated.action.hive.ACTION hive) {
        assertEquals(1, hive.getPrepare().getDelete().size());
        assertEquals(1, hive.getConfiguration().getProperty().size());
        assertEquals(2, hive.getParam().size());
    }

    private void assertShell(final org.apache.oozie.fluentjob.api.generated.action.shell.ACTION shell) {
        assertEquals("echo", shell.getExec());
        assertEquals(1, shell.getArgument().size());
    }

    private void assertSpark(final org.apache.oozie.fluentjob.api.generated.action.spark.ACTION spark) {
        assertEquals(1, spark.getPrepare().getDelete().size());
        assertEquals(1, spark.getConfiguration().getProperty().size());
        assertEquals(2, spark.getArg().size());
    }

    private void assertSqoop(final org.apache.oozie.fluentjob.api.generated.action.sqoop.ACTION sqoop) {
        assertEquals(1, sqoop.getPrepare().getDelete().size());
        assertEquals(1, sqoop.getConfiguration().getProperty().size());
    }

    private void assertSsh(final org.apache.oozie.fluentjob.api.generated.action.ssh.ACTION ssh) {
        assertEquals("foo@bar.com", ssh.getHost());
        assertEquals("uploaddata", ssh.getCommand());
        assertEquals(2, ssh.getArgs().size());
    }

    /**
     * Tests whether a programmatically built JAXB element tree is serialized correctly to xml.
     *
     * @throws JAXBException If an error was encountered while creating the <tt>JAXBContext</tt>
     *         or during the marshalling.
     */
    @Test
    public void marshallingWorkflowProducesCorrectXml() throws JAXBException, URISyntaxException, IOException,
                                                 ParserConfigurationException, SAXException {
        final WORKFLOWAPP programmaticallyCreatedWfApp = getWfApp();
        final String outputXml = marshalWorkflowApp(programmaticallyCreatedWfApp, GENERATED_PACKAGES_WORKFLOW);

        final Diff diff = DiffBuilder.compare(Input.fromURL(getClass().getResource(WORKFLOW_MAPREDUCE_ACTION)))
                .withTest(Input.fromString(outputXml))
                .ignoreComments()
                .withDifferenceEvaluator(DifferenceEvaluators.chain(
                        DifferenceEvaluators.Default,
                        new IgnoreWhitespaceInTextValueDifferenceEvaluator(),
                        new IgnoreNamespacePrefixDifferenceEvaluator()))
                .build();

        assertFalse(diff.hasDifferences());
    }

    @Test
    public void testMarshallingWorkflowWithAllActionTypesWorks() throws JAXBException, SAXException,
            URISyntaxException, UnsupportedEncodingException {
        final WORKFLOWAPP wf = unmarshalWorkflowWithAllActionTypes();
        final String outputXml = marshalWorkflowApp(wf, GENERATED_PACKAGES_ALL);

        final Diff diff = DiffBuilder.compare(Input.fromURL(getClass().getResource(WORKFLOW_ALL_ACTIONS)))
                .withTest(Input.fromString(outputXml))
                .ignoreComments()
                .withDifferenceEvaluator(DifferenceEvaluators.chain(
                        DifferenceEvaluators.Default,
                        new IgnoreWhitespaceInTextValueDifferenceEvaluator(),
                        new IgnoreNamespacePrefixDifferenceEvaluator()))
                .build();

        assertFalse("unmarshalled and marshalled workflow XMLs differ", diff.hasDifferences());
    }

    private static class IgnoreWhitespaceInTextValueDifferenceEvaluator implements DifferenceEvaluator {
        @Override
        public ComparisonResult evaluate(final Comparison comparison, final ComparisonResult comparisonResult) {
            // We want to ignore whitespace differences in TEXT_VALUE comparisons but not anywhere else,
            // for example not in attribute names.
            if (isTextValueComparison(comparison) && expectedAndActualValueTrimmedAreEqual(comparison)) {
                return ComparisonResult.EQUAL;
            } else {
                return comparisonResult;
            }
        }

        private boolean isTextValueComparison(final Comparison comparison) {
            return comparison.getType().equals(ComparisonType.TEXT_VALUE);
        }

        private boolean expectedAndActualValueTrimmedAreEqual(final Comparison comparison) {
            final String expectedNodeValue = comparison.getControlDetails().getTarget().getNodeValue();
            final String actualNodeValue = comparison.getTestDetails().getTarget().getNodeValue();

            if (expectedNodeValue == null || actualNodeValue == null) {
                return false;
            }

            return expectedNodeValue.trim().equals(actualNodeValue.trim());
        }
    }

    private static class IgnoreNamespacePrefixDifferenceEvaluator implements DifferenceEvaluator {

        @Override
        public ComparisonResult evaluate(final Comparison comparison, final ComparisonResult comparisonResult) {
            if (isElementNodeComparison(comparison)) {
                return ComparisonResult.EQUAL;
            }

            return comparisonResult;
        }

        private boolean isElementNodeComparison(final Comparison comparison) {
            return comparison.getType().equals(ComparisonType.NAMESPACE_PREFIX);
        }
    }

    private WORKFLOWAPP unmarshalWorkflowWithAllActionTypes() throws SAXException, JAXBException, URISyntaxException {
        final JAXBContext jc = JAXBContext.newInstance(GENERATED_PACKAGES_ALL);
        final Unmarshaller u = jc.createUnmarshaller();
        final Schema wfSchema = getSchema();
        u.setSchema(wfSchema);

        final URL wfUrl = getClass().getResource(WORKFLOW_ALL_ACTIONS);
        final JAXBElement element = (JAXBElement) u.unmarshal(wfUrl);

        return (WORKFLOWAPP) element.getValue();
    }

    private Schema getSchema() throws SAXException, URISyntaxException {
        final SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        return sf.newSchema(new Source [] {
                getStreamSource("/oozie-common-1.0.xsd"),
                getStreamSource("/distcp-action-1.0.xsd"),
                getStreamSource("/email-action-0.2.xsd"),
                getStreamSource("/hive2-action-1.0.xsd"),
                getStreamSource("/hive-action-1.0.xsd"),
                getStreamSource("/oozie-sla-0.2.xsd"),
                getStreamSource("/oozie-workflow-1.0.xsd"),
                getStreamSource("/shell-action-1.0.xsd"),
                getStreamSource("/spark-action-1.0.xsd"),
                getStreamSource("/sqoop-action-1.0.xsd"),
                getStreamSource("/ssh-action-0.2.xsd")
        });
    }

    private Source getStreamSource(final String resourceURI) throws URISyntaxException {
        return new StreamSource(getClass().getResource(resourceURI).toExternalForm());
    }

    private String marshalWorkflowApp(final WORKFLOWAPP wfApp, final String packages)
            throws JAXBException, UnsupportedEncodingException {
        final JAXBElement wfElement = new ObjectFactory().createWorkflowApp(wfApp);

        final JAXBContext jc = JAXBContext.newInstance(packages);
        final Marshaller m =  jc.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        m.marshal(wfElement, out);

        return out.toString(Charset.defaultCharset().toString());
    }

    private WORKFLOWAPP getWfApp() {
        final START start = new START();
        start.setTo("mr-node");

        final KILL kill = new KILL();
        kill.setName("fail");
        kill.setMessage("Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");

        final END end = new END();
        end.setName("end");

        final WORKFLOWAPP wfApp = new WORKFLOWAPP();
        wfApp.setName("jaxb-example-wf");
        wfApp.setStart(start);
        wfApp.getDecisionOrForkOrJoin().add(getMapReduceAction());
        wfApp.getDecisionOrForkOrJoin().add(kill);
        wfApp.setEnd(end);

        return wfApp;
    }

    private ACTION getMapReduceAction() {
        final ACTION action = new ACTION();

        action.setName("mr-node");
        action.setMapReduce(getMapreduce());

        final ACTIONTRANSITION okTransition = new ACTIONTRANSITION();
        okTransition.setTo("end");
        action.setOk(okTransition);

        final ACTIONTRANSITION errorTransition = new ACTIONTRANSITION();
        errorTransition.setTo("fail");
        action.setError(errorTransition);

        return action;
    }

    private MAPREDUCE getMapreduce() {
        final MAPREDUCE mr = new MAPREDUCE();

        mr.setResourceManager("${resourceManager}");
        mr.setNameNode("${nameNode}");
        mr.setPrepare(getPrepare());
        mr.setConfiguration(getConfiguration());

        return mr;
    }

    private CONFIGURATION getConfiguration() {
        final String[][] nameValuePairs = {
                {"mapred.job.queue.name", "${queueName}"},
                {"mapred.mapper.class", "org.apache.oozie.example.SampleMapper"},
                {"mapred.reducer.class", "org.apache.oozie.example.SampleReducer"},
                {"mapred.map.tasks", "1"},
                {"mapred.input.dir", "/user/${wf:user()}/${examplesRoot}/input-data/text"},
                {"mapred.output.dir", "/user/${wf:user()}/${examplesRoot}/output-data/${outputDir}"}
        };

        final CONFIGURATION config = new CONFIGURATION();
        final List<CONFIGURATION.Property> properties = config.getProperty();

        for (final String[] pair : nameValuePairs) {
            final CONFIGURATION.Property property = new CONFIGURATION.Property();
            property.setName(pair[0]);
            property.setValue(pair[1]);

            properties.add(property);
        }

        return config;
    }

    private PREPARE getPrepare() {
        final PREPARE prepare = new PREPARE();

        final DELETE delete = new DELETE();
        delete.setPath("${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/${outputDir}");

        prepare.getDelete().add(delete);

        return prepare;
    }
}
