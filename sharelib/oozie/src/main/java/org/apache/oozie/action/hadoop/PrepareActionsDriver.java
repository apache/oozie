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

import org.apache.hadoop.conf.Configuration;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Utility class to perform operations on the prepare block of Workflow
 *
 */
@Deprecated
public class PrepareActionsDriver {
    private static final PrepareActionsHandler prepareHandler = new PrepareActionsHandler(new LauncherURIHandlerFactory(null));

    /**
     * Method to parse the prepare XML and execute the corresponding prepare actions
     *
     * @param prepareXML Prepare XML block in string format
     * @throws LauncherException
     */
    static void doOperations(String prepareXML, Configuration conf)
            throws IOException, SAXException, ParserConfigurationException, LauncherException {
        prepareHandler.prepareAction(prepareXML, conf);
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
