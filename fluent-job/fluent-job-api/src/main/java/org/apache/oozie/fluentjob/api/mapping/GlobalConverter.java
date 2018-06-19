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

import com.google.common.base.Preconditions;
import org.apache.oozie.fluentjob.api.generated.workflow.CONFIGURATION;
import org.apache.oozie.fluentjob.api.generated.workflow.GLOBAL;
import org.apache.oozie.fluentjob.api.generated.workflow.LAUNCHER;
import org.apache.oozie.fluentjob.api.generated.workflow.ObjectFactory;
import org.apache.oozie.fluentjob.api.workflow.Global;
import org.dozer.DozerConverter;
import org.dozer.Mapper;
import org.dozer.MapperAware;

/**
 * A {@link DozerConverter} converting from {@link Global} to JAXB {@link GLOBAL}.
 */
public class GlobalConverter extends DozerConverter<Global, GLOBAL> implements MapperAware {
    private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();

    private Mapper mapper;

    public GlobalConverter() {
        super(Global.class, GLOBAL.class);
    }

    @Override
    public GLOBAL convertTo(final Global source, GLOBAL destination) {
        if (source == null) {
            return null;
        }

        destination = ensureDestination(destination);

        mapFields(source, destination);

        return destination;
    }

    private GLOBAL ensureDestination(final GLOBAL destination) {
        if (destination == null) {
            return OBJECT_FACTORY.createGLOBAL();
        }

        return destination;
    }

    private void mapFields(final Global source, final GLOBAL destination) {
        destination.setResourceManager(source.getResourceManager());
        destination.setNameNode(source.getNameNode());
        destination.getJobXml().addAll(source.getJobXmls());

        mapLauncher(source, destination);

        mapConfiguration(source, destination);
    }

    private void mapLauncher(final Global source, final GLOBAL destination) {
        if (source.getLauncher() != null) {
            destination.setLauncher(checkAndGetMapper().map(source.getLauncher(), LAUNCHER.class));
        }
    }

    private void mapConfiguration(final Global source, final GLOBAL destination) {
        if (source.getConfiguration() != null) {
            destination.setConfiguration(checkAndGetMapper().map(source.getConfiguration(), CONFIGURATION.class));
        }
    }

    @Override
    public Global convertFrom(final GLOBAL source, final Global destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }

    private Mapper checkAndGetMapper() {
        Preconditions.checkNotNull(mapper, "mapper should be set");
        return mapper;
    }

    @Override
    public void setMapper(final Mapper mapper) {
        this.mapper = mapper;
    }
}
