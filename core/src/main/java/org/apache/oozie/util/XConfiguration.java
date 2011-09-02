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

import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Extends Hadoop Configuration providing a new constructor which reads an XML configuration from an InputStream.
 * <p/>
 * OConfiguration(InputStream is).
 */
public class XConfiguration extends Configuration {

    /**
     * Create an empty configuration.
     * <p/>
     * Default values are not loaded.
     */
    public XConfiguration() {
        super(false);
    }

    /**
     * Create a configuration from an InputStream.
     * <p/>
     * Code canibalized from <code>Configuration.loadResource()</code>.
     *
     * @param is inputstream to read the configuration from.
     * @throws IOException thrown if the configuration could not be read.
     */
    public XConfiguration(InputStream is) throws IOException {
        this();
        parse(is);
    }

    /**
     * Create a configuration from an Reader.
     * <p/>
     * Code canibalized from <code>Configuration.loadResource()</code>.
     *
     * @param reader reader to read the configuration from.
     * @throws IOException thrown if the configuration could not be read.
     */
    public XConfiguration(Reader reader) throws IOException {
        this();
        parse(reader);
    }

    /**
     * Create an configuration from a Properties instance.
     *
     * @param props Properties instance to get all properties from.
     */
    public XConfiguration(Properties props) {
        this();
        for (Map.Entry entry : props.entrySet()) {
            set((String) entry.getKey(), (String) entry.getValue());
        }

    }

    /**
     * Return a Properties instance with the configuration properties.
     *
     * @return a Properties instance with the configuration properties.
     */
    public Properties toProperties() {
        Properties props = new Properties();
        for (Map.Entry<String, String> entry : this) {
                props.setProperty(entry.getKey(), entry.getValue());
        }
        return props;
    }

    /**
     * This is a stop gap fix for <link href="https://issues.apache.org/jira/browse/HADOOP-4416">HADOOP-4416</link>.
     */
    public Class<?> getClassByName(String name) throws ClassNotFoundException {
        return super.getClassByName(name.trim());
    }

    /**
     * Copy configuration key/value pairs from one configuration to another if a property exists in the target, it
     * gets replaced.
     *
     * @param source source configuration.
     * @param target target configuration.
     */
    public static void copy(Configuration source, Configuration target) {
        for (Map.Entry<String, String> entry : source) {
            target.set(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Injects configuration key/value pairs from one configuration to another if the key does not exist
     * in the target configuration.
     *
     * @param source    source configuration.
     * @param target target configuration.
     */
    public static void injectDefaults(Configuration source, Configuration target) {
        for (Map.Entry<String, String> entry : source) {
            if (target.get(entry.getKey()) == null) {
                target.set(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Returns a new Configuration with all values trimmed.
     *
     * @param conf the configuraton to trim.
     * @return a new Configuration with all values trimmed.
     */
    public static Configuration trim(Configuration conf) {
        XConfiguration trimmed = new XConfiguration();
        for (Map.Entry<String, String> entry : conf) {
            trimmed.set(entry.getKey(), entry.getValue().trim());
        }
        return trimmed;
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private void parse(InputStream is) throws IOException {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            // ignore all comments inside the xml file
            docBuilderFactory.setIgnoringComments(true);
            DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
            Document doc = builder.parse(is);
            parseDocument(doc);
        }
        catch (SAXException e) {
            throw new IOException(e);
        }
        catch (ParserConfigurationException e) {
            throw new IOException(e);
        }
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private void parse(Reader reader) throws IOException {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            // ignore all comments inside the xml file
            docBuilderFactory.setIgnoringComments(true);
            DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
            Document doc = builder.parse(new InputSource(reader));
            parseDocument(doc);
        }
        catch (SAXException e) {
            throw new IOException(e);
        }
        catch (ParserConfigurationException e) {
            throw new IOException(e);
        }
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private void parseDocument(Document doc) throws IOException {
        try {
            Element root = doc.getDocumentElement();
            if (!"configuration".equals(root.getTagName())) {
                throw new IOException("bad conf file: top-level element not <configuration>");
            }
            NodeList props = root.getChildNodes();
            for (int i = 0; i < props.getLength(); i++) {
                Node propNode = props.item(i);
                if (!(propNode instanceof Element)) {
                    continue;
                }
                Element prop = (Element) propNode;
                if (!"property".equals(prop.getTagName())) {
                    throw new IOException("bad conf file: element not <property>");
                }
                NodeList fields = prop.getChildNodes();
                String attr = null;
                String value = null;
                for (int j = 0; j < fields.getLength(); j++) {
                    Node fieldNode = fields.item(j);
                    if (!(fieldNode instanceof Element)) {
                        continue;
                    }
                    Element field = (Element) fieldNode;
                    if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
                        attr = ((Text) field.getFirstChild()).getData().trim();
                    }
                    if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
                        value = ((Text) field.getFirstChild()).getData();
                    }
                }

                if (attr != null && value != null) {
                    set(attr, value);
                }
            }

        }
        catch (DOMException e) {
            throw new IOException(e);
        }
    }

    /**
     * Return a string with the configuration in XML format.
     *
     * @return a string with the configuration in XML format.
     */
    public String toXmlString() {
        return toXmlString(true);
    }

    public String toXmlString(boolean prolog) {
        String xml;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            this.writeXml(baos);
            baos.close();
            xml =  new String(baos.toByteArray());
        }
        catch (IOException ex) {
            throw new RuntimeException("It should not happen, " + ex.getMessage(), ex);
        }
        if (!prolog) {
            xml = xml.substring(xml.indexOf("<configuration>"));
        }
        return xml;
    }
}