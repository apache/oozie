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

package org.apache.oozie.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.schema.ResourceResolver;
import org.apache.xerces.xni.XMLResourceIdentifier;
import org.apache.xerces.xni.XNIException;
import org.apache.xerces.xni.parser.XMLEntityResolver;
import org.apache.xerces.xni.parser.XMLInputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;


/**
 * Service that loads Oozie workflow definition schema and registered extension
 * schemas.
 */

public class SchemaService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "SchemaService.";

    public static final String WF_CONF_SCHEMAS = CONF_PREFIX + "wf.schemas";

    public static final String WF_CONF_EXT_SCHEMAS = CONF_PREFIX + "wf.ext.schemas";

    public static final String COORD_CONF_SCHEMAS = CONF_PREFIX + "coord.schemas";

    public static final String COORD_CONF_EXT_SCHEMAS = CONF_PREFIX + "coord.ext.schemas";

    public static final String BUNDLE_CONF_SCHEMAS = CONF_PREFIX + "bundle.schemas";

    public static final String BUNDLE_CONF_EXT_SCHEMAS = CONF_PREFIX + "bundle.ext.schemas";

    public static final String SLA_CONF_SCHEMAS = CONF_PREFIX + "sla.schemas";

    public static final String SLA_CONF_EXT_SCHEMAS = CONF_PREFIX + "sla.ext.schemas";

    @Deprecated
    public static final String SLA_NAME_SPACE_URI = "uri:oozie:sla:0.1";

    public static final String SLA_NAMESPACE_URI_2 = "uri:oozie:sla:0.2";

    public static final String COORDINATOR_NAMESPACE_URI_1 = "uri:oozie:coordinator:0.1";

    private Schema wfSchema;

    private Schema coordSchema;

    private Schema bundleSchema;

    private Schema slaSchema;

    private SchemaFactory schemaFactory;

    private static NoXMLEntityResolver xmlEntityResolver;

    private Schema loadSchema(String baseSchemas, String extSchema) throws SAXException, IOException {
        Set<String> schemaNames = new HashSet<String>();
        String[] schemas = ConfigurationService.getStrings(baseSchemas);
        if (schemas != null) {
            for (String schema : schemas) {
                schema = schema.trim();
                if (!schema.isEmpty()) {
                    schemaNames.add(schema);
                }
            }
        }
        schemas = ConfigurationService.getStrings(extSchema);
        if (schemas != null) {
            for (String schema : schemas) {
                schema = schema.trim();
                if (!schema.isEmpty()) {
                    schemaNames.add(schema);
                }
            }
        }
        List<StreamSource> sources = new ArrayList<StreamSource>();
        for (String schemaName : schemaNames) {
            StreamSource s = new StreamSource(IOUtils.getResourceAsStream(schemaName, -1));
            s.setSystemId(schemaName);
            sources.add(s);
        }
        schemaFactory.setResourceResolver(new ResourceResolver());
        return schemaFactory.newSchema(sources.toArray(new StreamSource[sources.size()]));
    }

    /**
     * Initialize the service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the service could not be initialized.
     */
    @Override
    public void init(Services services) throws ServiceException {
        try {
            schemaFactory = createSchemaFactory();
            wfSchema = loadSchema(WF_CONF_SCHEMAS, WF_CONF_EXT_SCHEMAS);
            coordSchema = loadSchema(COORD_CONF_SCHEMAS, COORD_CONF_EXT_SCHEMAS);
            bundleSchema = loadSchema(BUNDLE_CONF_SCHEMAS, BUNDLE_CONF_EXT_SCHEMAS);
            slaSchema = loadSchema(SLA_CONF_SCHEMAS, SLA_CONF_EXT_SCHEMAS);
            xmlEntityResolver = new NoXMLEntityResolver();
        }
        catch (SAXException ex) {
            throw new ServiceException(ErrorCode.E0130, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0131, ex.getMessage(), ex);
        }
    }

    /**
     * Creates schema factory
     * @return
     * @throws SAXNotRecognizedException
     * @throws SAXNotSupportedException
     */
    private SchemaFactory createSchemaFactory() throws SAXNotRecognizedException, SAXNotSupportedException {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING,true);
        return factory;
    }

    /**
     * Return the public interface of the service.
     *
     * @return {@link SchemaService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return SchemaService.class;
    }

    /**
     * Destroy the service.
     */
    @Override
    public void destroy() {
        wfSchema = null;
        bundleSchema = null;
        slaSchema = null;
        coordSchema = null;
    }

    /**
     * Return the schema for XML validation of application definitions.
     *
     * @param schemaName Name of schema definition (i.e.
     *        WORKFLOW/COORDINATOR/BUNDLE)
     * @return the schema for XML validation of application definitions.
     */
    public Schema getSchema(SchemaName schemaName) {
        Schema returnSchema = null;
        if (schemaName == SchemaName.WORKFLOW) {
            returnSchema = wfSchema;
        }
        else if (schemaName == SchemaName.COORDINATOR) {
            returnSchema = coordSchema;
        }
        else if (schemaName == SchemaName.BUNDLE) {
            returnSchema = bundleSchema;
        }
        else if (schemaName == SchemaName.SLA_ORIGINAL) {
            returnSchema = slaSchema;
        }
        else {
            throw new RuntimeException("No schema found with name " + schemaName);
        }
        return returnSchema;
    }

    public enum SchemaName {
        WORKFLOW(1), COORDINATOR(2), SLA_ORIGINAL(3), BUNDLE(4);
        private final int id;

        private SchemaName(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }

    /**
     * Returns validator for schema
     * @param schemaName
     * @return
     * @throws SAXException
     */
    public Validator getValidator(SchemaName schemaName) throws SAXException {
        return getValidator(getSchema(schemaName));
    }

    /**
     * Returns validator for schema
     * @param schema
     * @return
     * @throws SAXException
     */
    public static Validator getValidator(Schema schema) throws SAXException {
        Validator validator = schema.newValidator();
        validator.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        validator.setFeature("http://xml.org/sax/features/external-general-entities", false);
        validator.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        validator.setProperty("http://apache.org/xml/properties/internal/entity-resolver", xmlEntityResolver);
        return validator;
    }

    private static class NoXMLEntityResolver implements XMLEntityResolver {
        @Override
        public XMLInputSource resolveEntity(XMLResourceIdentifier xmlResourceIdentifier) throws XNIException, IOException {
            throw new IOException("DOCTYPE is disallowed when the feature http://apache.org/xml/features/disallow-doctype-decl "
                    + "set to true.");
        }
    }
}
