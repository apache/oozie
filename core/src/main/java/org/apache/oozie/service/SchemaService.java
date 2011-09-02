/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.IOUtils;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Service that loads Oozie workflow definition schema and registered extension schemas.
 */
public class SchemaService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "SchemaService.";

    public static final String WF_CONF_EXT_SCHEMAS = CONF_PREFIX + "wf.ext.schemas";

    public static final String COORD_CONF_EXT_SCHEMAS = CONF_PREFIX + "coord.ext.schemas";

    public static final String SLA_CONF_EXT_SCHEMAS = CONF_PREFIX + "sla.ext.schemas";

    public static final String SLA_NAME_SPACE_URI = "uri:oozie:sla:0.1";

    private Schema wfSchema;

    private Schema coordSchema;

    private Schema slaSchema;

    private static final String OOZIE_WORKFLOW_XSD[] = {"oozie-workflow-0.1.xsd", "oozie-workflow-0.2.xsd"};
    private static final String OOZIE_COORDINATOR_XSD[] = {"oozie-coordinator-0.1.xsd"};
    private static final String OOZIE_SLA_SEMANTIC_XSD[] = {"gms-oozie-sla-0.1.xsd"};

    private Schema loadSchema(Configuration conf, String[] baseSchemas, String extSchema) throws SAXException,
            IOException {
        List<StreamSource> sources = new ArrayList<StreamSource>();
        for (String baseSchema : baseSchemas) {
            sources.add(new StreamSource(IOUtils.getResourceAsStream(baseSchema, -1)));
        }
        String[] schemas = conf.getStrings(extSchema);
        if (schemas != null) {
            for (String schema : schemas) {
                schema = schema.replace("\n", "").trim();
                if (schema.length() > 0) {
                    sources.add(new StreamSource(IOUtils.getResourceAsStream(schema, -1)));
                }
            }
        }
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        return factory.newSchema(sources.toArray(new StreamSource[sources.size()]));
    }

    /**
     * Initialize the service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the service could not be initialized.
     */
    public void init(Services services) throws ServiceException {
        try {
            wfSchema = loadSchema(services.getConf(), OOZIE_WORKFLOW_XSD, WF_CONF_EXT_SCHEMAS);
            coordSchema = loadSchema(services.getConf(), OOZIE_COORDINATOR_XSD, COORD_CONF_EXT_SCHEMAS);
            slaSchema = loadSchema(services.getConf(), OOZIE_SLA_SEMANTIC_XSD, SLA_CONF_EXT_SCHEMAS);
        }
        catch (SAXException ex) {
            throw new ServiceException(ErrorCode.E0130, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0131, ex.getMessage(), ex);
        }
    }

    /**
     * Return the public interface of the service.
     *
     * @return {@link SchemaService}.
     */
    public Class<? extends Service> getInterface() {
        return SchemaService.class;
    }

    /**
     * Destroy the service.
     */
    public void destroy() {
        wfSchema = null;
    }

    /**
     * Return the schema for XML validation of application definitions.
     *
     * @param schemaName: Name of schema definition (i.e. WORKFLOW/COORDINATOR)
     * @return the schema for XML validation of application definitions.
     */
    public Schema getSchema(SchemaName schemaName) {
        if (schemaName == SchemaName.WORKFLOW) {
            return wfSchema;
        }
        else {
            if (schemaName == SchemaName.COORDINATOR) {
                return coordSchema;
            }
            else {
                if (schemaName == SchemaName.SLA_ORIGINAL) {
                    return slaSchema;
                }
                else {
                    throw new RuntimeException("No schema found with name " + schemaName);
                }
            }
        }
    }

    public enum SchemaName {
        WORKFLOW(1), COORDINATOR(2), SLA_ORIGINAL(3);
        private int id;

        private SchemaName(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }
}
