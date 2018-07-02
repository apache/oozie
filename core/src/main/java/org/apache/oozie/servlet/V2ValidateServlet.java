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

package org.apache.oozie.servlet;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.cli.OozieCLIException;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.IOUtils;
import org.json.simple.JSONObject;
import org.xml.sax.SAXException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.util.Arrays;

public class V2ValidateServlet extends JsonRestServlet {
    private static final String INSTRUMENTATION_NAME = "v2validate";

    private static final ResourceInfo RESOURCE_INFO =
            new ResourceInfo("", Arrays.asList("POST"), Arrays.asList(
                    new ParameterInfo(RestConstants.FILE_PARAM, String.class, true, Arrays.asList("POST")),
                    new ParameterInfo(RestConstants.USER_PARAM, String.class, true, Arrays.asList("POST"))));
    public static final String VALID_WORKFLOW_APP = "Valid workflow-app";

    public V2ValidateServlet() {
        super(INSTRUMENTATION_NAME, RESOURCE_INFO);
    }

    /**
     * Validate workflow definition.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        validateContentType(request, RestConstants.XML_CONTENT_TYPE);

        String file = request.getParameter(RestConstants.FILE_PARAM);
        String user = request.getParameter(RestConstants.USER_PARAM);

        stopCron();

        StringWriter stringWriter = new StringWriter();
        if (file.startsWith("hdfs://")) {
            try {
                URI uri = new URI(file);
                HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
                Configuration fsConf = has.createConfiguration(uri.getAuthority());
                FileSystem fs = has.createFileSystem(user, uri, fsConf);

                Path path = new Path(uri.getPath());
                IOUtils.copyCharStream(new InputStreamReader(fs.open(path), Charsets.UTF_8), stringWriter);
            } catch (Exception e) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0505,
                        "File does not exist, "+ file);
            }
        }
        else {
            IOUtils.copyCharStream(new InputStreamReader(request.getInputStream(), Charsets.UTF_8), stringWriter);
        }
        try {
            validate(stringWriter.toString());
        } catch (Exception e) {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0701,
                    file + ", " + e.toString());
        }

        JSONObject json = createJSON(VALID_WORKFLOW_APP);
        startCron();
        sendJsonResponse(response, HttpServletResponse.SC_OK, json);
    }

    public void validate(String xml) throws Exception {
        SchemaService schemaService = Services.get().get(SchemaService.class);
        Schema[] schemas = {schemaService.getSchema(SchemaService.SchemaName.WORKFLOW),
                schemaService.getSchema(SchemaService.SchemaName.COORDINATOR),
                schemaService.getSchema(SchemaService.SchemaName.BUNDLE),
                schemaService.getSchema(SchemaService.SchemaName.SLA_ORIGINAL)};

        Exception exception = null;
        for (int i = 0; i < schemas.length; i++) {
            try{
                validateSchema(schemas[i], new StringReader(xml));
                exception = null;
                break;
            } catch (SAXException e) {
                if (i == 0) {
                    exception = e;
                }
                // Check the root element declaration(workflow-app, coordinator-app, bundle-app).
                // If invalid, move to next schema validation.
                if (!e.getMessage().contains("cvc-elt.1")) {
                    exception = e;
                    break;
                }
            } catch (Exception e) {
                exception = e;
                break;
            }
        }
        if (exception !=  null) {
            throw exception;
        }
    }

    private void validateSchema(Schema schema, Reader src) throws SAXException, IOException, OozieCLIException{
        Validator validator = SchemaService.getValidator(schema);
        validator.validate(new StreamSource(src));
    }

    private JSONObject createJSON(String content) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(JsonTags.VALIDATE, content);
        return jsonObject;
    }
}