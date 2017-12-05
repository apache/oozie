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

package org.apache.oozie.util.graph;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.oozie.client.WorkflowJob;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;

/**
 * Class to generate and plot runtime workflow DAG.
 * <p>
 * Since it delegates to {@link WorkflowGraphHandler} and a {@link GraphRenderer}, it is the single entry point when changing graph
 * generation behavior.
 */
public class GraphGenerator {
    public static final String SAX_FEATURE_EXTERNAL_GENERAL_ENTITIES = "http://xml.org/sax/features/external-general-entities";
    public static final String SAX_FEATURE_EXTERNAL_PARAMETER_ENTITIES = "http://xml.org/sax/features/external-parameter-entities";
    public static final String SAX_FEATURE_DISALLOW_DOCTYPE_DECL = "http://apache.org/xml/features/disallow-doctype-decl";
    private final GraphRenderer graphRenderer;
    private final String xml;
    private final WorkflowJob job;
    private final boolean showKill;

    /**
     * Constructor Inversion of Control-style for better testability.
     * @param xml The workflow definition XML
     * @param job Current status of the job
     * @param showKill Flag to whether show 'kill' node
     * @param graphRenderer the renderer
     */
    public GraphGenerator(final String xml, final WorkflowJob job, final boolean showKill, final GraphRenderer graphRenderer) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(xml), "xml can't be empty");
        Preconditions.checkArgument(job != null, "WorkflowJob can't be null");

        this.xml = xml;
        this.job = job;
        this.showKill = showKill;
        this.graphRenderer = graphRenderer;
    }

    /**
     * Stream the generated PNG, DOT or SVG stream to the caller. Note that closing the {@link OutputStream} is the responsibility
     * of the caller.
     *
     * @param out the {@link OutputStream} to use on streaming
     * @param outputFormat The output format to apply when rendering
     * @throws ParserConfigurationException if the parser is not configured properly
     * @throws SAXException in case of XML error
     * @throws IOException in case of any other IO related issues
     */
    public void write(final OutputStream out, final OutputFormat outputFormat)
            throws ParserConfigurationException, SAXException, IOException {
        final XMLReader xmlReader = newXmlReader();
        xmlReader.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);

        xmlReader.setContentHandler(
                new WorkflowGraphHandler(out, outputFormat, job, showKill, graphRenderer));

        try (final StringReader stringReader = new StringReader(xml)) {
            xmlReader.parse(new InputSource(stringReader));
        }
    }

    private XMLReader newXmlReader() throws ParserConfigurationException, SAXException {
        final SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setFeature(SAX_FEATURE_EXTERNAL_GENERAL_ENTITIES, false);
        spf.setFeature(SAX_FEATURE_EXTERNAL_PARAMETER_ENTITIES, false);
        spf.setFeature(SAX_FEATURE_DISALLOW_DOCTYPE_DECL, true);
        spf.setNamespaceAware(true);

        final SAXParser saxParser = spf.newSAXParser();
        final XMLReader xmlReader = saxParser.getXMLReader();
        xmlReader.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);

        return xmlReader;
    }
}