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
package org.apache.oozie.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.SchemaService.SchemaName;
import org.jdom.Comment;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * XML utility methods.
 */
public class XmlUtils {
    public static final String SLA_NAME_SPACE_URI = "uri:oozie:sla:0.1";

    private static class NoExternalEntityEntityResolver implements EntityResolver {

        public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
            return new InputSource(new ByteArrayInputStream(new byte[0]));
        }

    }

    private static SAXBuilder createSAXBuilder() {
        SAXBuilder saxBuilder = new SAXBuilder();

        //THIS IS NOT WORKING
        //saxBuilder.setFeature("http://xml.org/sax/features/external-general-entities", false);

        //INSTEAD WE ARE JUST SETTING AN EntityResolver that does not resolve entities
        saxBuilder.setEntityResolver(new NoExternalEntityEntityResolver());
        return saxBuilder;
    }

    /**
     * Remove comments from any Xml String.
     *
     * @param xmlStr XML string to remove comments.
     * @return String after removing comments.
     * @throws JDOMException thrown if an error happend while XML parsing.
     */
    public static String removeComments(String xmlStr) throws JDOMException {
        if (xmlStr == null) {
            return null;
        }
        try {
            SAXBuilder saxBuilder = createSAXBuilder();
            Document document = saxBuilder.build(new StringReader(xmlStr));
            removeComments(document);
            return prettyPrint(document.getRootElement()).toString();
        }
        catch (IOException ex) {
            throw new RuntimeException("It should not happen, " + ex.getMessage(), ex);
        }
    }

    private static void removeComments(List l) {
        for (Iterator i = l.iterator(); i.hasNext();) {
            Object node = i.next();
            if (node instanceof Comment) {
                i.remove();
            }
            else {
                if (node instanceof Element) {
                    removeComments(((Element) node).getContent());
                }
            }
        }
    }

    private static void removeComments(Document doc) {
        removeComments(doc.getContent());
    }

    /**
     * Parse a string assuming it is a valid XML document and return an JDOM Element for it.
     *
     * @param xmlStr XML string to parse.
     * @return JDOM element for the parsed XML string.
     * @throws JDOMException thrown if an error happend while XML parsing.
     */
    public static Element parseXml(String xmlStr) throws JDOMException {
        ParamChecker.notNull(xmlStr, "xmlStr");
        try {
            SAXBuilder saxBuilder = createSAXBuilder();
            Document document = saxBuilder.build(new StringReader(xmlStr));
            return document.getRootElement();
        }
        catch (IOException ex) {
            throw new RuntimeException("It should not happen, " + ex.getMessage(), ex);
        }
    }

    /**
     * Parse a inputstream assuming it is a valid XML document and return an JDOM Element for it.
     *
     * @param is inputstream to parse.
     * @return JDOM element for the parsed XML string.
     * @throws JDOMException thrown if an error happend while XML parsing.
     * @throws IOException thrown if an IO error occurred.
     */
    public static Element parseXml(InputStream is) throws JDOMException, IOException {
        ParamChecker.notNull(is, "is");
        SAXBuilder saxBuilder = createSAXBuilder();
        Document document = saxBuilder.build(is);
        return document.getRootElement();
    }

    /**
     * //TODO move this to action registry method Return the value of an attribute from the root element of an XML
     * document.
     *
     * @param filePath path of the XML document.
     * @param attributeName attribute to retrieve value for.
     * @return value of the specified attribute.
     */
    public static String getRootAttribute(String filePath, String attributeName) {
        ParamChecker.notNull(filePath, "filePath");
        ParamChecker.notNull(attributeName, "attributeName");
        SAXBuilder saxBuilder = createSAXBuilder();
        try {
            Document doc = saxBuilder.build(Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath));
            return doc.getRootElement().getAttributeValue(attributeName);
        }
        catch (JDOMException e) {
            throw new RuntimeException();
        }
        catch (IOException e) {
            throw new RuntimeException();
        }
    }

    /**
     * Pretty print string representation of an XML document that generates the pretty print on lazy mode when the
     * {@link #toString} method is invoked.
     */
    public static class PrettyPrint {
        private String str;
        private Element element;

        private PrettyPrint(String str) {
            this.str = str;
        }

        private PrettyPrint(Element element) {
            this.element = ParamChecker.notNull(element, "element");
        }

        /**
         * Return the pretty print representation of an XML document.
         *
         * @return the pretty print representation of an XML document.
         */
        @Override
        public String toString() {
            if (str != null) {
                return str;
            }
            else {
                XMLOutputter outputter = new XMLOutputter();
                StringWriter stringWriter = new StringWriter();
                outputter.setFormat(Format.getPrettyFormat());
                try {
                    outputter.output(element, stringWriter);
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                return stringWriter.toString();
            }
        }
    }

    /**
     * Return a pretty print string for a JDOM Element.
     *
     * @param element JDOM element.
     * @return pretty print of the given JDOM Element.
     */
    public static PrettyPrint prettyPrint(Element element) {
        return new PrettyPrint(element);

    }

    /**
     * Return a pretty print string for a XML string. If the given string is not valid XML it returns the original
     * string.
     *
     * @param xmlStr XML string.
     * @return prettyprint of the given XML string or the original string if the given string is not valid XML.
     */
    public static PrettyPrint prettyPrint(String xmlStr) {
        try {
            return new PrettyPrint(parseXml(xmlStr));
        }
        catch (Exception e) {
            return new PrettyPrint(xmlStr);
        }
    }

    /**
     * Return a pretty print string for a Configuration object.
     *
     * @param conf Configuration object.
     * @return prettyprint of the given Configuration object.
     */
    public static PrettyPrint prettyPrint(Configuration conf) {
        Element root = new Element("configuration");
        for (Map.Entry<String, String> entry : conf) {
            Element property = new Element("property");
            Element name = new Element("name");
            name.setText(entry.getKey());
            Element value = new Element("value");
            value.setText(entry.getValue());
            property.addContent(name);
            property.addContent(value);
            root.addContent(property);
        }
        return new PrettyPrint(root);
    }

    /**
     * Schema validation for a given xml. <p/>
     *
     * @param schema for validation
     * @param xml to be validated
     */
    public static void validateXml(Schema schema, String xml) throws SAXException, IOException {

        Validator validator = schema.newValidator();
        validator.validate(new StreamSource(new ByteArrayInputStream(xml.getBytes())));
    }

    /**
     * Create schema object for the given xsd
     *
     * @param is inputstream to schema.
     * @return the schema object.
     */
    public static Schema createSchema(InputStream is) {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        StreamSource src = new StreamSource(is);
        try {
            return factory.newSchema(src);
        }
        catch (SAXException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static void validateData(String xmlData, SchemaName xsdFile) throws SAXException, IOException {
        if (xmlData == null || xmlData.length() == 0) {
            return;
        }
        javax.xml.validation.Schema schema = Services.get().get(SchemaService.class).getSchema(xsdFile);
        validateXml(schema, xmlData);
    }

    /**
     * Convert Properties to string
     *
     * @param props
     * @return xml string
     * @throws IOException
     */
    public static String writePropToString(Properties props) throws IOException {
        try {
            org.w3c.dom.Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            org.w3c.dom.Element conf = doc.createElement("configuration");
            doc.appendChild(conf);
            conf.appendChild(doc.createTextNode("\n"));
            for (Enumeration e = props.keys(); e.hasMoreElements();) {
                String name = (String) e.nextElement();
                Object object = props.get(name);
                String value;
                if (object instanceof String) {
                    value = (String) object;
                }
                else {
                    continue;
                }
                org.w3c.dom.Element propNode = doc.createElement("property");
                conf.appendChild(propNode);

                org.w3c.dom.Element nameNode = doc.createElement("name");
                nameNode.appendChild(doc.createTextNode(name.trim()));
                propNode.appendChild(nameNode);

                org.w3c.dom.Element valueNode = doc.createElement("value");
                valueNode.appendChild(doc.createTextNode(value.trim()));
                propNode.appendChild(valueNode);

                conf.appendChild(doc.createTextNode("\n"));
            }

            Source source = new DOMSource(doc);
            StringWriter stringWriter = new StringWriter();
            Result result = new StreamResult(stringWriter);
            TransformerFactory factory = TransformerFactory.newInstance();
            Transformer transformer = factory.newTransformer();
            transformer.transform(source, result);

            return stringWriter.getBuffer().toString();
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Escape characters for text appearing as XML data, between tags.
     * <P/>
     * The following characters are replaced with corresponding character entities :
     * '<' to '&lt';
     * '>' to '&gt';
     * '&' to '&amp;'
     * '"' to '&quot;'
     * "'" to "&#039;"
     * <P/>
     * Note that JSTL's {@code <c:out>} escapes the exact same set of characters as this method.
     */
    public static String escapeCharsForXML(String aText) {
        final StringBuilder result = new StringBuilder();
        final StringCharacterIterator iterator = new StringCharacterIterator(aText);
        char character = iterator.current();
        while (character != CharacterIterator.DONE) {
            if (character == '<') {
                result.append("&lt;");
            }
            else if (character == '>') {
                result.append("&gt;");
            }
            else if (character == '\"') {
                result.append("&quot;");
            }
            else if (character == '\'') {
                result.append("&#039;");
            }
            else if (character == '&') {
                result.append("&amp;");
            }
            else {
                // the char is not a special one
                // add it to the result as is
                result.append(character);
            }
            character = iterator.next();
        }
        return result.toString();
    }

}
