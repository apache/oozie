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

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
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
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extends Hadoop Configuration providing a new constructor which reads an XML configuration from an InputStream. <p>
 * OConfiguration(InputStream is).
 */
public class XConfiguration extends Configuration {

    public static final String CONFIGURATION_SUBSTITUTE_DEPTH = "oozie.configuration.substitute.depth";

    private boolean restrictSystemProperties = true;
    private boolean restrictParser = true;
    /**
     * Create an empty configuration. <p> Default values are not loaded.
     */
    public XConfiguration() {
        super(false);
        initSubstituteDepth();
    }

    /**
     * Create a configuration from an InputStream. <p> Code canibalized from <code>Configuration.loadResource()</code>.
     *
     * @param is inputstream to read the configuration from.
     * @param restrictParser whether to restrict the parser
     * @throws IOException thrown if the configuration could not be read.
     */
    public XConfiguration(InputStream is, boolean restrictParser) throws IOException {
        this();
        this.restrictParser = restrictParser;
        parse(is);
    }

    /**
     * Create a configuration from an InputStream. <p> Code canibalized from <code>Configuration.loadResource()</code>.
     *
     * @param is inputstream to read the configuration from.
     * @throws IOException thrown if the configuration could not be read.
     */
    public XConfiguration(InputStream is) throws IOException {
        this(is, true);
    }

    /**
     * Create a configuration from an Reader. <p> Code canibalized from <code>Configuration.loadResource()</code>.
     *
     * @param reader reader to read the configuration from.
     * @param restrictParser whether to restrict the parser
     * @throws IOException thrown if the configuration could not be read.
     */
    public XConfiguration(Reader reader, boolean restrictParser) throws IOException {
        this();
        this.restrictParser = restrictParser;
        parse(reader);
    }

    /**
     * Create a configuration from an Reader. <p> Code canibalized from <code>Configuration.loadResource()</code>.
     *
     * @param reader reader to read the configuration from.
     * @throws IOException thrown if the configuration could not be read.
     */
    public XConfiguration(Reader reader) throws IOException {
        this(reader, true);
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

    // overriding get() & substitueVars from Configuration to honor defined variables
    // over system properties
    //wee need this because substituteVars() is a private method and does not behave like virtual
    //in Configuration
    /**
     * Get the value of the <code>name</code> property, <code>null</code> if
     * no such property exists.
     *
     * Values are processed for <a href="#VariableExpansion">variable expansion</a>
     * before being returned.
     *
     * @param name the property name.
     * @return the value of the <code>name</code> property,
     *         or null if no such property exists.
     */
    @Override
    public String get(String name) {
        return substituteVars(getRaw(name));
    }

    /**
     * Get the value of the <code>name</code> property. If no such property
     * exists, then <code>defaultValue</code> is returned.
     *
     * @param name property name.
     * @param defaultValue default value.
     * @return property value, or <code>defaultValue</code> if the property
     *         doesn't exist.
     */
    @Override
    public String get(String name, String defaultValue) {
        String value = getRaw(name);
        if (value == null) {
            value = defaultValue;
        }
        else {
            value = substituteVars(value);
        }
        return value;
    }

    private static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
    private static int MAX_SUBST = 20;
    protected static volatile boolean initalized = false;
    private static void initSubstituteDepth() {
        if (!initalized && Services.get() != null && Services.get().get(ConfigurationService.class) != null) {
            MAX_SUBST = ConfigurationService.getInt(CONFIGURATION_SUBSTITUTE_DEPTH);
            initalized = true;
        }
    }

    private String substituteVars(String expr) {
        if (expr == null) {
            return null;
        }
        Matcher match = varPat.matcher("");
        String eval = expr;
        int s = 0;
        while (MAX_SUBST == -1 || s < MAX_SUBST ) {
            match.reset(eval);
            if (!match.find()) {
                return eval;
            }
            String var = match.group();
            var = var.substring(2, var.length() - 1); // remove ${ .. }

            String val = getRaw(var);

            if (val == null && !this.restrictSystemProperties) {
                val =  System.getProperty(var);
            }

            if (val == null) {
                return eval; // return literal ${var}: var is unbound
            }
            // substitute
            eval = eval.substring(0, match.start()) + val + eval.substring(match.end());
            s++;
        }
        throw new IllegalStateException("Variable substitution depth too large: " + MAX_SUBST + " " + expr);
    }

    /**
     * This is a stop gap fix for HADOOP-4416.
     */
    public Class<?> getClassByName(String name) throws ClassNotFoundException {
        return super.getClassByName(name.trim());
    }

    /**
     * Copy configuration key/value pairs from one configuration to another if a property exists in the target, it gets
     * replaced.
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
     * Injects configuration key/value pairs from one configuration to another if the key does not exist in the target
     * configuration.
     *
     * @param source source configuration.
     * @param target target configuration.
     */
    public static void injectDefaults(Configuration source, Configuration target) {
        if (source != null) {
            for (Map.Entry<String, String> entry : source) {
                if (target.get(entry.getKey()) == null) {
                    target.set(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    /**
     * Returns a new XConfiguration with all values trimmed.
     *
     * @return a new XConfiguration with all values trimmed.
     */
    public XConfiguration trim() {
        XConfiguration trimmed = new XConfiguration();
        for (Map.Entry<String, String> entry : this) {
            trimmed.set(entry.getKey(), entry.getValue().trim());
        }
        return trimmed;
    }

    /**
     * Returns a new XConfiguration instance with all inline values resolved.
     *
     * @return a new XConfiguration instance with all inline values resolved.
     */
    public XConfiguration resolve() {
        XConfiguration resolved = new XConfiguration();
        for (Map.Entry<String, String> entry : this) {
            resolved.set(entry.getKey(), get(entry.getKey()));
        }
        return resolved;
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private void parse(InputStream is) throws IOException {
        try {
            Document doc = getDocumentBuilder().parse(is);
            parseDocument(doc);

        }
        catch (SAXException | ParserConfigurationException e) {
            throw new IOException(e);
        }
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private void parse(Reader reader) throws IOException {
        try {
            Document doc = getDocumentBuilder().parse(new InputSource(reader));
            parseDocument(doc);
        }
        catch (SAXException | ParserConfigurationException e) {
            throw new IOException(e);
        }
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private void parseDocument(Document doc) throws IOException {
        Element root = doc.getDocumentElement();
        if (!"configuration".equals(root.getLocalName())) {
            throw new IOException("bad conf file: top-level element not <configuration>");
        }
        processNodes(root);
    }

    // Cannibalized from Hadoop <code>Configuration.loadResource()</code>.
    private void processNodes(Element root) throws IOException {
        try {
            NodeList props = root.getChildNodes();
            for (int i = 0; i < props.getLength(); i++) {
                Node propNode = props.item(i);
                if (!(propNode instanceof Element)) {
                    continue;
                }
                Element prop = (Element) propNode;
                if (prop.getLocalName().equals("configuration")) {
                    processNodes(prop);
                    continue;
                }
                if (!"property".equals(prop.getLocalName())) {
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
                    if ("name".equals(field.getLocalName()) && field.hasChildNodes()) {
                        attr = ((Text) field.getFirstChild()).getData().trim();
                    }
                    if ("value".equals(field.getLocalName()) && field.hasChildNodes()) {
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
            xml = new String(baos.toByteArray(), Charsets.UTF_8);
        }
        catch (IOException ex) {
            throw new RuntimeException("Xml writing failed, " + ex.getMessage(), ex);
        }
        if (!prolog) {
            xml = xml.substring(xml.indexOf("<configuration>"));
        }
        return xml;
    }

    /**
     * Get the comma delimited values of the name property as an array of trimmed Strings. If no such property is specified then
     * null is returned.
     *
     * @param name property name.
     * @return property value as an array of trimmed Strings, or null.
     */
    public String[] getTrimmedStrings(String name) {
        String[] values = getStrings(name);
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                values[i] = values[i].trim();
            }
        }
        return values;
    }

    private DocumentBuilder getDocumentBuilder() throws ParserConfigurationException {
        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        docBuilderFactory.setNamespaceAware(true);
        docBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl",true);
        docBuilderFactory.setXIncludeAware(true);
        if(this.restrictParser) {
            docBuilderFactory.setXIncludeAware(false);
            //Redundant with disallow-doctype, but just in case
            docBuilderFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            docBuilderFactory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            docBuilderFactory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        }
        docBuilderFactory.setExpandEntityReferences(false);
        // ignore all comments inside the xml file
        docBuilderFactory.setIgnoringComments(true);
        return docBuilderFactory.newDocumentBuilder();
    }

    /**
     * Restrict the parser
     * @param restrictParser
     */
    public void setRestrictParser(boolean restrictParser) {
     this.restrictParser = restrictParser;
    }

    public boolean getRestrictParser() {
        return restrictParser;
    }
    /**
     * Restrict reading property from System.getProperty()
     * @param restrictSystemProperties
     */
    public void setRestrictSystemProperties(boolean restrictSystemProperties) {
        this.restrictSystemProperties = restrictSystemProperties;
    }

    public boolean getRestrictSystemProperties() {
        return restrictSystemProperties;
    }



}
