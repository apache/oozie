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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.ErrorCode;
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
public class WorkflowSchemaService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "WorkflowSchemaService.";

    public static final String CONF_EXT_SCHEMAS = CONF_PREFIX + "ext.schemas";

    private Schema dagSchema;

    private static final String OOZIE_WORKFLOW_XSD = "oozie-workflow-0.1.xsd";

    private Schema loadSchema(Configuration conf) throws SAXException, IOException {
        List<StreamSource> sources = new ArrayList<StreamSource>();
        sources.add(new StreamSource(IOUtils.getResourceAsStream(OOZIE_WORKFLOW_XSD, -1)));
        String[] schemas = conf.getStrings(CONF_EXT_SCHEMAS);
        if (schemas != null) {
            for (String schema : schemas) {
                sources.add(new StreamSource(IOUtils.getResourceAsStream(schema, -1)));
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
            dagSchema = loadSchema(services.getConf());
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
     * @return {@link WorkflowSchemaService}.
     */
    public Class<? extends Service> getInterface() {
        return WorkflowSchemaService.class;
    }

    /**
     * Destroy the service.
     */
    public void destroy() {
        dagSchema = null;
    }

    /**
     * Return the schema for XML validation of application definitions.
     *
     * @return the schema for XML validation of application definitions.
     */
    public Schema getSchema() {
        return dagSchema;
    }

}
