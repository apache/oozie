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

package org.apache.oozie.fluentjob.api.mapping;

import org.apache.oozie.fluentjob.api.generated.workflow.CONFIGURATION;
import org.apache.oozie.fluentjob.api.generated.workflow.ObjectFactory;
import org.dozer.DozerConverter;

import java.util.Map;

/**
 * A {@link DozerConverter} converting from {@link Map} to JAXB {@link CONFIGURATION}.
 */
public class MapToConfigurationPropertyConverter extends DozerConverter<Map, CONFIGURATION> {
    private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();

    public MapToConfigurationPropertyConverter() {
        super(Map.class, CONFIGURATION.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CONFIGURATION convertTo(final Map source, CONFIGURATION destination) {
        destination = ensureConfiguration(destination);

        for (final Object entryObject : source.entrySet()) {
            final Map.Entry<String, String> entry = (Map.Entry<String, String>) entryObject;
            final String key = entry.getKey();
            final String value = entry.getValue();

            final CONFIGURATION.Property property = createProperty(key, value);

            destination.getProperty().add(property);
        }

        return destination;
    }

    private CONFIGURATION ensureConfiguration(CONFIGURATION destination) {
        if (destination == null) {
            destination = OBJECT_FACTORY.createCONFIGURATION();
        }
        return destination;
    }

    private CONFIGURATION.Property createProperty(final String key, final String value) {
        final CONFIGURATION.Property property = OBJECT_FACTORY.createCONFIGURATIONProperty();

        property.setName(key);
        property.setValue(value);

        return property;
    }

    @Override
    public Map convertFrom(final CONFIGURATION source, final Map destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }
}
