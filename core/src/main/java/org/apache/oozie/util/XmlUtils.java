/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.util;

import org.jdom.Element;
import org.jdom.Document;
import org.jdom.JDOMException;
import org.jdom.output.XMLOutputter;
import org.jdom.output.Format;
import org.jdom.input.SAXBuilder;
import org.xml.sax.SAXException;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

/**
 * XML utility methods.
 */
public class XmlUtils {

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
     * //TODO move this to action registry method
     * Return the value of an attribute from the root element of an XML document.
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
     * Schema validation for a given xml.
     * <p/>
     *
     *  @param schema for validation
     *  @param xml to be validated
     */
    public static void validateXml(Schema schema, String xml) throws SAXException,IOException{

        Validator validator = schema.newValidator();
        validator.validate(new StreamSource(new ByteArrayInputStream(xml.getBytes())));
    }

    /** Create schema object for the given xsd
     *
     * @param is inputstream to schema.
     * @return the schema object.
     */
    public static Schema createSchema(InputStream is){
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        StreamSource src = new StreamSource(is);
        try {
            return factory.newSchema(src);
        }
        catch (SAXException e) {
            throw new RuntimeException(e.getMessage(),e);
        }
    }
}