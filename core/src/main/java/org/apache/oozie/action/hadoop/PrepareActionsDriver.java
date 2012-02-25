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

import org.xml.sax.SAXException;
import org.apache.oozie.action.hadoop.FileSystemActions;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Utility class to perform operations on the prepare block of Workflow
 *
 */
public class PrepareActionsDriver {

    /**
     * Method to parse the prepare XML and execute the corresponding prepare actions
     *
     * @param prepareXML Prepare XML block in string format
     * @throws LauncherException
     */
    static void doOperations(String prepareXML) throws LauncherException {
        try {
            Document doc = getDocumentFromXML(prepareXML);
            doc.getDocumentElement().normalize();

            // Get the list of child nodes, basically, each one corresponding to a separate action
            NodeList nl = doc.getDocumentElement().getChildNodes();
            FileSystemActions fsActions = new FileSystemActions();

            for (int i = 0; i < nl.getLength(); ++i) {
                String commandType = "";
                /* Logic to find the command type goes here
                commandType = ..........;
                */
                // As of now, the available prepare action is of type hdfs. Hence, assigning the value directly
                commandType = "hdfs";
                if (commandType.equalsIgnoreCase("hdfs")) {
                    fsActions.execute(nl.item(i));
                } /*else if(commandType.equalsIgnoreCase("hcat")) {     //Other command types go here
                    hCatActions.execute(nl.item(i));
                  }*/
            }
        } catch (IOException ioe) {
            throw new LauncherException(ioe.getMessage(), ioe);
        } catch (SAXException saxe) {
            throw new LauncherException(saxe.getMessage(), saxe);
        } catch (ParserConfigurationException pce) {
            throw new LauncherException(pce.getMessage(), pce);
        }
    }

    // Method to return the document from the prepare XML block
    static Document getDocumentFromXML(String prepareXML) throws ParserConfigurationException, SAXException,
            IOException {
        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
        InputStream is = new ByteArrayInputStream(prepareXML.getBytes("UTF-8"));
        return docBuilder.parse(is);
    }
}
