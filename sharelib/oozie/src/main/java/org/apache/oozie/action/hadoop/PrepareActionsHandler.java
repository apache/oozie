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
package org.apache.oozie.action.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class PrepareActionsHandler {
    private final LauncherURIHandlerFactory factory;

    @VisibleForTesting
    PrepareActionsHandler(final LauncherURIHandlerFactory factory) {
        this.factory = factory;
    }

    /**
     * Method to parse the prepare XML and execute the corresponding prepare actions
     *
     * @param prepareXML Prepare XML block in string format
     * @param conf the configuration
     * @throws IOException if there is an IO error during prepare action
     * @throws SAXException in case of xml parsing error
     * @throws ParserConfigurationException if the parser is not well configured
     * @throws LauncherException in case of error
     */
    void prepareAction(String prepareXML, Configuration conf)
            throws IOException, SAXException, ParserConfigurationException, LauncherException {
        Document doc = getDocumentFromXML(prepareXML);
        doc.getDocumentElement().normalize();

        // Get the list of child nodes, basically, each one corresponding to a separate action
        NodeList nl = doc.getDocumentElement().getChildNodes();

        for (int i = 0; i < nl.getLength(); ++i) {
            Node n = nl.item(i);
            String operation = n.getLocalName();
            if (n.getAttributes() == null || n.getAttributes().getNamedItem("path") == null) {
                continue;
            }
            String pathStr = n.getAttributes().getNamedItem("path").getNodeValue().trim();
            // use Path to avoid URIsyntax error caused by square bracket in glob
            URI uri = new Path(pathStr).toUri();
            LauncherURIHandler handler = factory.getURIHandler(uri, conf);
            execute(operation, uri, handler, conf);
        }
    }

    private void execute(String operation, URI uri, LauncherURIHandler handler, Configuration conf)
            throws LauncherException {

        switch (operation) {
            case "delete":
                handler.delete(uri, conf);
                break;

            case "mkdir":
                handler.create(uri, conf);
                break;

            default:
                System.out.println("Warning: unknown prepare operation " + operation + " -- skipping");
            }
    }

    // Method to return the document from the prepare XML block
    static Document getDocumentFromXML(String prepareXML) throws ParserConfigurationException, SAXException,
            IOException {
        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        docBuilderFactory.setNamespaceAware(true);
        // support for includes in the xml file
        docBuilderFactory.setXIncludeAware(true);
        // ignore all comments inside the xml file
        docBuilderFactory.setIgnoringComments(true);
        docBuilderFactory.setExpandEntityReferences(false);
        docBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
        InputStream is = new ByteArrayInputStream(prepareXML.getBytes("UTF-8"));
        return docBuilder.parse(is);
    }

}
