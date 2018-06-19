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

import org.apache.oozie.fluentjob.api.generated.action.spark.DELETE;
import org.apache.oozie.fluentjob.api.generated.action.spark.MKDIR;
import org.apache.oozie.fluentjob.api.generated.action.spark.ObjectFactory;
import org.apache.oozie.fluentjob.api.generated.action.spark.PREPARE;
import org.apache.oozie.fluentjob.api.action.Delete;
import org.apache.oozie.fluentjob.api.action.Mkdir;
import org.apache.oozie.fluentjob.api.action.Prepare;
import org.dozer.DozerConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link DozerConverter} converting from {@link Prepare} to JAXB {@link PREPARE}.
 */
public class SparkPrepareConverter extends DozerConverter<Prepare, PREPARE> {
    private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();

    public SparkPrepareConverter() {
        super(Prepare.class, PREPARE.class);
    }

    @Override
    public PREPARE convertTo(final Prepare source, PREPARE destination) {
        if (source == null) {
            return null;
        }

        destination = ensureDestination(destination);

        mapDeletes(source, destination);

        mapMkdirs(source, destination);

        return destination;
    }

    private PREPARE ensureDestination(final PREPARE destination) {
        if (destination == null) {
            return OBJECT_FACTORY.createPREPARE();
        }
        return destination;
    }

    private void mapDeletes(final Prepare source, final PREPARE destination) {
        if (source.getDeletes() != null) {
            final List<DELETE> targetDeletes = new ArrayList<>();

            for (final Delete sourceDelete : source.getDeletes()) {
                final DELETE targetDelete = OBJECT_FACTORY.createDELETE();
                targetDelete.setPath(sourceDelete.getPath());
                targetDeletes.add(targetDelete);
            }

            destination.setDelete(targetDeletes);
        }
    }

    private void mapMkdirs(final Prepare source, final PREPARE destination) {
        if (source.getMkdirs() != null) {
            final List<MKDIR> targetMkdirs = new ArrayList<>();

            for (final Mkdir sourceMkDir: source.getMkdirs()) {
                final MKDIR targetMkDir = OBJECT_FACTORY.createMKDIR();
                targetMkDir.setPath(sourceMkDir.getPath());
                targetMkdirs.add(targetMkDir);
            }

            destination.setMkdir(targetMkdirs);
        }
    }

    @Override
    public Prepare convertFrom(final PREPARE source, final Prepare destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }
}
